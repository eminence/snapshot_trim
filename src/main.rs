
extern crate time;
extern crate core;
extern crate libc;
extern crate regex;



use regex::Regex;
use std::io::{BufReader, BufRead};
use time::Timespec;
use core::fmt::{Display,Formatter,Error};
use std::process::Command;
use std::process::Stdio;
use libc::funcs::c95::stdlib::exit;
use std::cmp::Ordering;


#[derive(Debug)]
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
            Err(e) => panic!(format!("{} - {}", e, snap.as_slice())),
            Ok(t) => t
        };
        Snapshot{snap:snap, state:SnapState::UNKNOWN, time:t.to_timespec()}
    }
    fn zfs_destroy(&self) {
        let mut zfs_proc = match Command::new("/sbin/zfs").arg("destroy").arg(self.snap.as_slice()).stdout(Stdio::piped()).spawn() {
            Ok(p) => p,
            Err(e) => panic!("failed to execute process: {}", e),
        };
        let exit_status = match zfs_proc.wait() {
            Err(e) => {panic!("Unable to destroy snapshot {}", e)},
            Ok(o) => o
        };
        if !exit_status.success() {
            panic!("Bad exit status {}", exit_status);
        }
        println!("Deleted {}", self.snap);
        //if return_code.unwrap() != 0 {
        //    println!("Unable to destroy snapshot!");
        //}
    }
}

impl Display for Snapshot {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        write!(fmt, "<Snapshot {:?} {:?}>", self.snap, self.state);
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



fn list_of_snaps(filesystem: &str) -> Vec<Snapshot> {

    let re = match Regex::new("@[0-9]{8}-[0-9]{4}") {
        Ok(re) => re,
        Err(err) => panic!("{}", err)
    };

    let mut snaps = std::vec::Vec::new();

    let mut zfs_proc = match Command::new("/sbin/zfs").arg("list").arg("-r").arg("-t").arg("snapshot").arg(filesystem).stdout(Stdio::piped()).spawn() {
          Ok(p) => p,
          Err(e) => panic!("failed to execute process: {}", e),
    };

    println!("New process is running");

    //let stdout_reader = BufferedReader::new(
    {
        let stdout = match zfs_proc.stdout {
            None => panic!("no stdout"),
            Some(ref mut t) => t
        };
        let mut buf_stdout = BufReader::new(stdout);

        loop {
            let mut std_out_line: String = String::new();
            match buf_stdout.read_line(&mut std_out_line) {
                Err(_) => break,
                Ok(_) => {
                    if std_out_line.is_empty() { break; }
                    let snapshot:&str = match std_out_line.as_slice().split(' ').nth(0) {
                        None => continue,
                        Some(s) => s
                    };
                    let m = re.find(snapshot);
                    if m.is_some() {
                        let (s,e) = m.unwrap();
                        let volume = snapshot.slice(0,s);
                        if volume == filesystem {
                            snaps.push(Snapshot::new(snapshot.slice(s+1,e),String::from_str(snapshot)));
                        }
                    }
                }
            };
        }

    }

    let return_code = zfs_proc.wait();
    if return_code.is_err() {
        println!("Warning! zfs has failed return code");
    }
    snaps.sort();
    println!("Found a total of {} snapshots!", snaps.len());
    return snaps;
}


fn period(t: f32) -> f32 {
    // bigger constant means more dense snapshots
    t/ 250.0f32
}


// algorithm by agrif (http://github.com/agrif/)
// See this excellent demo: http://overviewer.org/~agrif/snapshotvis/
// each snapshot (based on how old it is), will have a "radius" that indicates that any other
// snapshots within the radius should be deleted
fn collect(mut snaps: Vec<Snapshot>) -> Vec<Snapshot> {
    let now = time::now().to_timespec().sec;

    let mut idx = 0;
    let mut destroyed : i32 = 0;
    loop {
        if idx >= snaps.len() { break; } 
        let t:f32 = (now - snaps[idx].time.sec) as f32;
        let radius:f32 = period(t);
        let mut new_snaps = std::vec::Vec::new();
        let mut iidx = 0;
        for snap in snaps.into_iter() {
            if t - radius > (now - snap.time.sec) as f32 || (now - snap.time.sec) as f32 > t || idx == iidx {
                new_snaps.push(snap);
            } else {
                snap.zfs_destroy();
                destroyed += 1;
                if destroyed >= 50 {
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

    let snaps = list_of_snaps("storage/home/achin");
    collect(snaps);
    //for snap in snaps.iter() {
    //    println!("{}", snap);
    //}
}
