use crossbeam::channel::*;
use crossbeam::thread;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use std::thread::sleep;
use std::time::Duration;

struct F {
    idx: usize,
    val: usize,
}

const FRAME_CADENCE: Duration = Duration::from_millis(10);
const SCENECHANGE_ANALYSIS: Duration = Duration::from_millis(20);
const INVARANTS_BUILD: Duration = Duration::from_millis(70);
const RDO: Duration = Duration::from_millis(200);

const LOOKAHEAD: usize = 100;

fn producer(s: &thread::Scope, frames: usize, pb: ProgressBar) -> Receiver<F> {
    let (send, recv) = bounded(LOOKAHEAD * 2);
    s.spawn(move |_| {
        for idx in 0..frames {
            let f = F { idx, val: idx % 42 };

            std::thread::sleep(FRAME_CADENCE);

            pb.set_message(&format!("Frame {}", idx));
            pb.inc(1);
            send.send(f);
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
}

// Assumptions:
// the min-kf is always larger than the sub-gop size
impl SceneChange {
    fn split(&mut self, l: &[F]) -> Option<(usize, bool)> {
        let new_gop = self.frames != 0 && (self.frames % 100) == 0;
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

fn scenechange(s: &thread::Scope, r: Receiver<F>, pb: ProgressBar) -> Receiver<SB> {
    let (send, recv) = bounded(LOOKAHEAD / 7);

    s.spawn(move |_| {
        let mut lookahead = Vec::new();
        let mut sc = SceneChange::default();
        let mut gop = 0;
        for f in r.iter() {
            lookahead.push(f);

            if let Some((split_pos, end)) = sc.split(&lookahead) {
                let rem = lookahead.split_off(split_pos);
                if end {
                    gop += 1;
                }

                pb.set_message(&format!("gop {} sub gop of {}", gop, lookahead.len()));

                send.send(SB {
                    data: lookahead,
                    end_gop: end,
                });
                lookahead = rem;
                pb.inc(1);
            }
        }

        assert!(lookahead.len() <= 7);

        send.send(SB {
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

fn encode(s: &thread::Scope, r: Receiver<SB>, pb: ProgressBar) -> Receiver<Packet> {
    let (send, recv) = unbounded();
    s.spawn(move |_| {
        for sb in r.iter() {
            std::thread::sleep(INVARANTS_BUILD);
            pb.set_message(&format!(
                "Invariants built for {}..{}",
                sb.data[0].idx,
                sb.data.last().unwrap().idx
            ));

            for f in sb.data {
                sleep(RDO);
                pb.inc(1);
                send.send(Packet { idx: f.idx });
            }

            if sb.end_gop {
                pb.set_message(&format!("starting a new gop"));
            }
        }

        pb.finish_with_message("Complete");
    });

    recv
}

fn main() {
    let m = MultiProgress::new();

    let spinner_style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("{prefix:.bold.dim} {spinner} {msg}");

    thread::scope(|s| {
        let pb = m.add(ProgressBar::new(600));
        pb.set_style(spinner_style.clone());
        let f_recv = producer(s, 600, pb);

        let pb = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));
        let sb_recv = scenechange(s, f_recv, pb);

        let pb = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));
        let packet_recv = encode(s, sb_recv, pb);

        let pb = m.add(ProgressBar::new(!0).with_style(spinner_style.clone()));

        s.spawn(move |_| {
            for p in packet_recv.iter() {
                pb.set_message(&format!("Packet {}", p.idx));
                pb.inc(1);
            }
            pb.finish_with_message("Complete");
        });

        s.spawn(move |_| {
            m.join_and_clear().unwrap();
        });
    });
}
