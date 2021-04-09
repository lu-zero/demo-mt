use crossbeam::channel::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

struct F {
    idx: usize,
    _val: usize,
}

const FRAME_CADENCE: Duration = Duration::from_millis(10);
const SCENECHANGE_ANALYSIS: Duration = Duration::from_millis(20);
const INVARANTS_BUILD: Duration = Duration::from_millis(50);
const RDO: Duration = Duration::from_millis(100);

const MIN_KF: usize = 12;
const MAX_KF: usize = 100;

const LOOKAHEAD: usize = 50;
const FRAMES: usize = 500;
const WORKERS: usize = 8;

fn producer(s: &rayon::ScopeFifo, frames: usize, pb: ProgressBar) -> Receiver<F> {
    let (send, recv) = bounded(LOOKAHEAD * 2);
    s.spawn_fifo(move |_| {
        for idx in 0..frames {
            let f = F {
                idx,
                _val: idx % 42,
            };

            std::thread::sleep(FRAME_CADENCE);

            pb.set_message(&format!("Frame {}", idx));
            pb.inc(1);
            let _ = send.send(f);
        }

        pb.finish_with_message("Producer stopped");
    });

    recv
}

struct SB {
    data: Vec<F>,
    end_gop: bool,
}

#[derive(Default)]
struct SceneChange {
    frames: usize,
    kf: usize,
}

impl SceneChange {
    fn new() -> Self {
        SceneChange {
            frames: 0,
            kf: MAX_KF,
        }
    }
}

// Assumptions:
// the min-kf is always larger than the sub-gop size
impl SceneChange {
    fn split(&mut self, l: &[F]) -> Option<(usize, bool)> {
        let new_gop = self.frames != 0 && (self.frames % self.kf) == 0;
        self.frames += 1;

        if l.len() > 7 {
            Some((7, new_gop))
        } else if new_gop {
            Some((l.len(), true))
        } else {
            std::thread::sleep(SCENECHANGE_ANALYSIS);
            None
        }
    }
}

fn scenechange(s: &rayon::ScopeFifo, r: Receiver<F>, pb: ProgressBar) -> Receiver<SB> {
    let (send, recv) = bounded(LOOKAHEAD / 7);

    s.spawn_fifo(move |_| {
        let mut rng = thread_rng();
        let mut lookahead = Vec::new();
        let mut sc = SceneChange::new();
        let mut gop = 0;
        for f in r.iter() {
            lookahead.push(f);

            if let Some((split_pos, end)) = sc.split(&lookahead) {
                let rem = lookahead.split_off(split_pos);
                if end {
                    gop += 1;
                    sc.kf = rng.sample(Uniform::new(MIN_KF, MAX_KF));
                }

                pb.set_message(&format!("gop {} sub gop of {}", gop, lookahead.len()));

                let _ = send.send(SB {
                    data: lookahead,
                    end_gop: end,
                });
                lookahead = rem;
                pb.inc(1);
            }
        }

        assert!(lookahead.len() <= 7);

        let _ = send.send(SB {
            data: lookahead,
            end_gop: false,
        });

        pb.finish_with_message("Complete");
    });

    recv
}

struct Packet {
    idx: usize,
}

struct WorkLoad {
    sb_recv: Receiver<SB>,
    send: Sender<Packet>,
}

struct WorkerPool {
    recv_workers: Receiver<Sender<WorkLoad>>,
    send_reassemble: Sender<(usize, Receiver<Packet>)>,
    count: usize,
}

impl WorkerPool {
    fn new(
        s: &rayon::ScopeFifo,
        workers: usize,
        m: Arc<MultiProgress>,
    ) -> (Self, Receiver<(usize, Receiver<Packet>)>) {
        let (send_workers, recv_workers) = bounded(workers);
        let (send_reassemble, recv_reassemble) = unbounded();
        for w in 0..workers {
            let (send_workload, recv_workload) = unbounded::<WorkLoad>();
            let spinner_style = ProgressStyle::default_spinner()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
                .template("  {prefix:.bold.dim} {spinner} {msg}");

            let pb = m.add(ProgressBar::new(!0).with_style(spinner_style));

            let send_workload2 = send_workload.clone();
            let send_back = send_workers.clone();
            s.spawn_fifo(move |_| {
                pb.set_message(&format!("Starting worker {}", w));
                for wl in recv_workload.iter() {
                    let mut never_in = true;
                    for sb in wl.sb_recv.iter() {
                        use rayon::prelude::*;
                        std::thread::sleep(INVARANTS_BUILD);
                        pb.set_message(&format!(
                            "({:02}) Processing {}..{} {:?}",
                            w,
                            sb.data[0].idx,
                            sb.data.last().unwrap().idx,
                            rayon::current()
                        ));

                        for f in sb.data {
                            sleep(RDO);
                            (0..16).into_par_iter().for_each(|_| {
                                pb.inc(1);
                            });
                            let _ = wl.send.send(Packet { idx: f.idx });
                        }
                        never_in = false;
                    }
                    pb.set_message(&format!("Adding back {} {}", w, never_in));
                    let _ = send_back.send(send_workload2.clone());
                }
                pb.set_message(&format!("Complete {}", w));
            });
            let _ = send_workers.send(send_workload);
        }

        (
            WorkerPool {
                recv_workers,
                send_reassemble,
                count: 0,
            },
            recv_reassemble,
        )
    }

    fn get_worker(&mut self) -> Option<Sender<SB>> {
        self.recv_workers.recv().ok().map(|sender| {
            let (sb_send, sb_recv) = unbounded();
            let (send, recv) = unbounded();

            let _ = self.send_reassemble.send((self.count, recv));

            let wl = WorkLoad { sb_recv, send };

            let _ = sender.send(wl);

            self.count += 1;

            sb_send
        })
    }
}

fn reassemble(
    recv_reassemble: Receiver<(usize, Receiver<Packet>)>,
    s: &rayon::ScopeFifo,
) -> Receiver<Packet> {
    let (send_packet, receive_packet) = unbounded();

    s.spawn_fifo(move |_| {
        let mut pending = BTreeMap::new();
        let mut last_idx = 0;
        for (idx, recv) in recv_reassemble.iter() {
            pending.insert(idx, recv);
            while let Some(recv) = pending.remove(&last_idx) {
                for p in recv {
                    let _ = send_packet.send(p);
                }
            }
            last_idx += 1;
        }
    });

    receive_packet
}

fn encode(s: &rayon::ScopeFifo, r: Receiver<SB>, m: Arc<MultiProgress>) -> Receiver<Packet> {
    let pb = m.add(ProgressBar::new(!0));
    let (mut pool, recv) = WorkerPool::new(s, WORKERS, m);

    s.spawn_fifo(move |_| {
        let mut sb_send = pool.get_worker().unwrap();
        for sb in r.iter() {
            let end_gop = sb.end_gop;

            let _ = sb_send.send(sb);

            if end_gop {
                sb_send = pool.get_worker().unwrap();
            }
        }
        pb.println(&format!("Workers {}", pool.recv_workers.len()));
    });

    reassemble(recv, s)
}

fn main() {
    let m = Arc::new(MultiProgress::new());

    let spinner_style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("{prefix:.bold.dim} {spinner} {msg}");

    // producer, scenechange, encode, reassemble , packet_recv, ui
    let long_winded_threads = 6;
    let workers = WORKERS;

    let pool = rayon::ThreadPoolBuilder::new()
        // .start_handler(|idx| println!("Thread {}", idx))
        // .thread_name(|idx| format!("{}_usage", idx))
        .num_threads(long_winded_threads + workers)
        .build()
        .unwrap();

    let _ = pool.scope_fifo(|s| {
        let pb_producer = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));
        let pb_packet = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));
        pb_producer.tick();
        pb_packet.tick();

        let f_recv = producer(s, FRAMES, pb_producer);

        let pb = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));
        let sb_recv = scenechange(s, f_recv, pb);

        let m2 = Arc::clone(&m);
        let packet_recv = encode(s, sb_recv, m2);

        s.spawn_fifo(move |_| {
            let mut cur = 0;
            for p in packet_recv.iter() {
                pb_packet.set_message(&format!("Packet {}", p.idx));
                assert_eq!(cur, p.idx);
                pb_packet.inc(1);
                cur += 1;
            }
            pb_packet.finish_with_message("Complete");
        });

        s.spawn_fifo(move |_| {
            m.join_and_clear().unwrap();
        });
    });
}
