extern crate time;
//extern crate libc;
extern crate regex;
extern crate rustc_serialize;
extern crate toml;

use regex::Regex;
use std::io::{Read, BufReader, BufRead};
use time::Timespec;
use std::fmt::{Display,Formatter,Error};
use std::process::{Command, Stdio};
use std::convert::AsRef;
use std::cmp::Ordering;
use std::path::Path;
use std::fs::File;
use std::borrow::Borrow;
use std::str::FromStr;


#[derive(Debug, PartialEq, Eq)]
enum SnapState {
    EXISTS,
    DELETED
}

struct Snapshot {
    time: Timespec,
    snap: String,
    state: SnapState
}

impl Snapshot {
    fn new(timestamp: &str, snap: String) -> Snapshot {
        let t = match time::strptime(timestamp, "%Y%m%d-%H%M") {
            Err(e) => panic!(format!("{} - {}", e, snap)),
            Ok(t) => t
        };
        Snapshot{snap:snap, state:SnapState::EXISTS, time:t.to_timespec()}
    }
    fn zfs_destroy(&mut self) {
        if self.state == SnapState::DELETED {
            panic!("{} is already deleted!", self);
        }
        //println!("ZFS DESTROY {}", self.snap);
        let mut zfs_proc = match Command::new("/sbin/zfs").arg("destroy").arg(AsRef::<str>::as_ref(&(self.snap))).stdout(Stdio::piped()).spawn() {
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
        self.state = SnapState::DELETED;
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
                    let snapshot:&str = match AsRef::<str>::as_ref(&std_out_line).split(' ').nth(0) {
                        None => continue,
                        Some(s) => s
                    };
                    let m = re.find(snapshot);
                    if m.is_some() {
                        let m = m.unwrap();
                        let (s,e) = (m.start(), m.end());
                        let volume = &snapshot[0..s];
                        if volume == filesystem {
                            snaps.push(Snapshot::new(&snapshot[s+1..e],String::from_str(snapshot).unwrap()));
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


fn _period(t: f32) -> f32 {
    // bigger constant means more dense snapshots
    //t/ 250.0f32
    t / 50.0f32
}


// algorithm by agrif (http://github.com/agrif/)
// See this excellent demo: http://overviewer.org/~agrif/snapshotvis/
// each snapshot (based on how old it is), will have a "radius" that indicates that any other
// snapshots within the radius should be deleted
fn collect<F>(mut snaps: Vec<Snapshot>, period: F) -> Vec<Snapshot> 
    where F: Fn(f32) -> f32 {
    let now = time::now().to_timespec().sec;

    let mut idx = 0;
    let mut destroyed : i32 = 0;
    loop {
        if idx >= snaps.len() { break; } 
        let t:f32 = (now - snaps[idx].time.sec) as f32;
        let radius:f32 = period(t);
        let mut new_snaps = std::vec::Vec::new();
        let mut iidx = 0;
        for mut snap in snaps {
            if t - radius > (now - snap.time.sec) as f32 || (now - snap.time.sec) as f32 > t || idx == iidx {
                new_snaps.push(snap);
            } else {
                snap.zfs_destroy();
                destroyed += 1;
                //if destroyed >= 50 {
                //    unsafe {exit(0); }
                //}
            }
            iidx += 1;
        };
        snaps = new_snaps;
        //let it = snaps.move_iter().enumerate().filter(|&(i, snap)| {
        //    t - radius > (now - snap.time.sec) as f32 || (now - snap.time.sec) as f32 > t
        //});

        idx += 1;
    }
    println!("Deleted a total of {} snapshots", destroyed);
    return snaps

}


fn main() {

    let conf_path = Path::new("/storage/home/achin/.snapshot.toml");
    if !conf_path.exists() {
        panic!("{} Error: ~/.snapshot.toml doesn't exist");
    }

    let mut conf_file = match File::open(&conf_path) {
        Err(e) => panic!("Failed to open conf_path {}", e),
        Ok(f) => f
    };

    let mut toml_conf = String::new();
    conf_file.read_to_string(&mut toml_conf).unwrap();

    let parsed_toml = toml_conf.parse::<toml::Value>();

    if parsed_toml.is_err() {
        println!("Error parsing config file");
        return;
    }

    let main_data = match parsed_toml.unwrap() {
        toml::Value::Table(t) =>  t,
        x => panic!("Unexpected data in snapshot.toml: {:?}", x)
    };

    for (fs, data) in main_data {
        let datatable = match data {
            toml::Value::Table(d) => d,
            _ => panic!("Unexpected data type")
        };

        let period : i64 = match datatable.get("period") {
            Some(p) => p.as_integer().expect("Unexpected data type for period value"),
            None => panic!("Missing period")
        };

        println!("Scanning {}, purging with period={}", fs, period);
        let snaps = list_of_snaps(fs.borrow());
        collect(snaps, |x| x/period as f32);


    }

    // use 250 for storage/home/achin
    // use 15 for storage/home/achin/tmp

    //for snap in snaps.iter() {
    //    println!("{}", snap);
    //}
}
