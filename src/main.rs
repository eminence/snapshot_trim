#![feature(phase)]
extern crate std;
extern crate getopts;
extern crate regex;
extern crate time;
extern crate core;
extern crate libc;
extern crate semver;
#[phase(plugin)] extern crate regex_macros;
use regex::Regex;
use getopts::{optopt,optflag,getopts};
use std::os;
use std::io::BufferedReader;
use time::Timespec;
use core::fmt::{Show,Formatter,FormatError};
use std::io::Command;
use libc::funcs::c95::stdlib::exit;



static RE:Regex = regex!(r"@[0-9]{8}-[0-9]{4}");


#[deriving(Show)]
enum SnapState {
    UNKNOWN,
    SAVE,
    DELETE
}

struct Snapshot {
    time: Timespec,
    snap: String,
    state: SnapState
}

impl Snapshot {
    fn new(timestamp: &str, snap: String) -> Snapshot {
        let t = match time::strptime(timestamp, "%Y%m%d-%H%M") {
            Err(e) => fail!(format!("{} - {}", e, snap.as_slice())),
            Ok(t) => t
        };
        Snapshot{snap:snap, state:UNKNOWN, time:t.to_timespec()}
    }
    fn zfs_destroy(&self) {
        let mut zfs_proc = match Command::new("/sbin/zfs").arg("destroy").arg(self.snap.as_slice()).spawn() {
            Ok(p) => p,
            Err(e) => fail!("failed to execute process: {}", e),
        };
        let return_code = zfs_proc.wait();
        println!("Deleted {}", self.snap);
        //if return_code.unwrap() != 0 {
        //    println!("Unable to destroy snapshot!");
        //}
    }
}

impl Show for Snapshot {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), FormatError> {
        fmt.write(format!("<Snapshot {} {}>", self.snap, self.state).as_bytes());
        Ok(())
    }
}

impl Ord for Snapshot {
    fn cmp(&self, other: &Snapshot) -> Ordering { self.time.cmp(&other.time) }
}

impl PartialOrd for Snapshot {
    fn partial_cmp(&self, other: &Snapshot) -> Option<Ordering> { self.time.partial_cmp(&other.time) }
}

impl Eq for Snapshot { }

impl PartialEq for Snapshot {
    fn eq(&self, other: &Snapshot) -> bool {
        (self.time == other.time) && (self.snap == other.snap)
    }
}



fn list_of_snaps() -> Vec<Snapshot> {

    let mut snaps = std::vec::Vec::new();

    let mut zfs_proc = match Command::new("/sbin/zfs").args(["list", "-r", "-t", "snapshot", "storage/home/achin"]).spawn() {
          Ok(p) => p,
          Err(e) => fail!("failed to execute process: {}", e),
    };

    println!("New process is running: {}", zfs_proc.id());

    //let stdout_reader = BufferedReader::new(
    {
        let stdout = match zfs_proc.stdout {
            None => fail!("no stdout"),
            Some(ref mut t) => t.clone()
        };
        let mut buf_stdout = BufferedReader::new(stdout);

        loop {
            match buf_stdout.read_line() {
                Err(_) => break,
                Ok(s) => {
                    let snapshot:&str = match s.as_slice().split(' ').nth(0) {
                        None => continue,
                        Some(s) => s
                    };
                    let m = RE.find(snapshot);
                    if m.is_some() {
                        let (s,e) = m.unwrap();
                        snaps.push(Snapshot::new(snapshot.slice(s+1,e),snapshot.into_string()));
                    }
                }
            };
        }

    }

    let return_code = zfs_proc.wait();
    snaps.sort();
    println!("Found a total of {} snapshots!", snaps.len());
    return snaps;
}


fn period(t: int) -> f32 {
    t as f32 / 150.0f32
}

fn collect(mut snaps: Vec<Snapshot>) -> Vec<Snapshot> {
    let now = time::now().to_timespec().sec;
    let mut tr_vec : std::vec::Vec<(f32, f32)> = std::vec::Vec::new();

    let mut idx = 0;
    let mut destroyed = 0i;
    loop {
        if idx >= snaps.len() { break; } 
        let t:f32 = (now - snaps.get(idx).time.sec) as f32;
        let radius:f32 = period(t as int);
        let mut new_snaps = std::vec::Vec::new();
        let mut iidx = 0;
        for snap in snaps.move_iter() {
            if t - radius > (now - snap.time.sec) as f32 || (now - snap.time.sec) as f32 > t || idx == iidx {
                new_snaps.push(snap);
            } else {
                snap.zfs_destroy();
                destroyed += 1;
                if destroyed >= 25 {
                    unsafe {exit(0); }
                }
            }
            iidx += 1;
        };
        snaps = new_snaps;
        //let it = snaps.move_iter().enumerate().filter(|&(i, snap)| {
        //    t - radius > (now - snap.time.sec) as f32 || (now - snap.time.sec) as f32 > t
        //});

        idx += 1;
    }
    return snaps

}


fn main() {

    let args = os::args();

    let opts = [
        optopt("o", "", "output thingy", ""),
        optflag("h", "help", "help output")
            ];


    let matches = match getopts(args.tail(), opts) {
        Ok(m) => { m }
        Err(f) => { fail!(f.to_err_msg()) }
    };

    if matches.opt_present("h") || matches.opt_present("help") {
        //print_usage(program, opts);
        return;
    }


    let mut snaps = list_of_snaps();
    snaps = collect(snaps);
    for snap in snaps.iter() {
        println!("{}", snap);
    }
}
