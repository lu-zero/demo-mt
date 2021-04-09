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
const FRAMES: usize = 5000;
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

struct Worker {
    idx: usize,
    back: Sender<usize>,
    send: Sender<Packet>,
    pb: ProgressBar,
}

impl Worker {
    fn process(&self, sb: SB) {
        use rayon::prelude::*;
        std::thread::sleep(INVARANTS_BUILD);
        self.pb.set_message(&format!(
            "({:02}) Processing {}..{} tid {:?}",
            self.idx,
            sb.data[0].idx,
            sb.data.last().unwrap().idx,
            rayon::current()
        ));

        for f in sb.data {
            sleep(RDO);
            (0..16).into_par_iter().for_each(|_| {
                self.pb.inc(1);
            });
            let _ = self.send.send(Packet { idx: f.idx });
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.back.send(self.idx);
    }
}

struct WorkerPool {
    send_workers: Sender<usize>,
    recv_workers: Receiver<usize>,
    send_reassemble: Sender<(usize, Receiver<Packet>)>,
    m: Arc<MultiProgress>,
    count: usize,
}

impl WorkerPool {
    fn new(workers: usize, m: Arc<MultiProgress>) -> (Self, Receiver<(usize, Receiver<Packet>)>) {
        let (send_workers, recv_workers) = bounded(workers);
        let (send_reassemble, recv_reassemble) = unbounded();

        for w in 0..workers {
            let _ = send_workers.send(w);
        }

        (
            WorkerPool {
                send_workers,
                recv_workers,
                send_reassemble,
                m,
                count: 0,
            },
            recv_reassemble,
        )
    }

    fn get_worker(&mut self, s: &rayon::ScopeFifo) -> Option<Sender<SB>> {
        self.recv_workers.recv().ok().map(|idx| {
            let (sb_send, sb_recv) = unbounded();
            let (send, recv) = unbounded();

            let _ = self.send_reassemble.send((self.count, recv));

            let spinner_style = ProgressStyle::default_spinner()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
                .template("  {prefix:.bold.dim} {spinner} {msg}");

            let pb = self.m.add(ProgressBar::new(!0).with_style(spinner_style));

            let w = Worker {
                idx,
                back: self.send_workers.clone(),
                send,
                pb,
            };

            s.spawn_fifo(move |_| {
                for sb in sb_recv.iter() {
                    w.process(sb);
                }
            });

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
    let (mut pool, recv) = WorkerPool::new(WORKERS, m);

    let mut sb_send = pool.get_worker(s).unwrap();

    s.spawn_fifo(move |s| {
        for sb in r.iter() {
            let end_gop = sb.end_gop;

            let _ = sb_send.send(sb);

            if end_gop {
                sb_send = pool.get_worker(s).unwrap();
            }
        }
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
    let workers = 4;

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
