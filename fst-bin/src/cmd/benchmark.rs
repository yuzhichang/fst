use std::path::PathBuf;
use std::time::Instant;

use crate::util;
use crate::Error;

pub fn run(matches: &clap::ArgMatches) -> Result<(), Error> {
    Args::new(matches).and_then(|args| args.run())
}

#[derive(Debug)]
struct Args {
    fp_keys: PathBuf,
    fp_fst: PathBuf,
    delimiter: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DelimiterInvalidError;

impl std::error::Error for DelimiterInvalidError {}
impl std::fmt::Display for DelimiterInvalidError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "The provided value is no valid delimiter")
    }
}

impl Args {
    fn new(m: &clap::ArgMatches) -> Result<Args, Error> {
        Ok(Args {
            fp_keys: m.value_of_os("keys").map(PathBuf::from).unwrap(),
            fp_fst: m.value_of_os("fst").map(PathBuf::from).unwrap(),
            delimiter: m
                .value_of_lossy("delimiter")
                .map(|x| {
                    if x == "\\t" {
                        Ok('\t' as u8)
                    } else {
                        x.as_bytes()
                            .get(0)
                            .map(|y| *y)
                            .ok_or(DelimiterInvalidError)
                    }
                })
                .transpose()?,
        })
    }

    fn run(&self) -> Result<(), Error> {
        let mut queries = Vec::<(String, u64)>::new();
        let mut rdr = csv::ReaderBuilder::new()
        .delimiter(self.delimiter.unwrap_or(b','))
        .has_headers(false)
        .from_reader(util::get_reader(Some(&self.fp_keys))?);
        for row in rdr.deserialize() {
            let (key, val): (String, u64) = row?;
            queries.push((key, val));
        }

        let mut cnt : u64 = 0;
        let mut running = true;
        let begin = Instant::now();
        let fst = unsafe { util::mmap_fst(&self.fp_fst)? };
        while running {
            for (key, val) in &queries {
                match fst.get(key){
                    Some(res) => {
                        if res.value() != *val {
                            // -1 means expecting missing
                            let exp = if *val == u64::MAX{
                                "missing".to_string()
                            } else {
                                val.to_string()
                            };
                            panic!("Value of key {} mismatch, expect {}, got {}", key, exp, res.value());
                        }
                    },
                    _ => {
                        if *val!= u64::MAX {
                            panic!("Value of key {} mismatch, expect {}, got missing", key, val);
                        }
                    }
                }
                cnt+=1;
                if (cnt & 0xFFFF) == 0 {
                    let elapsed_time = Instant::now() - begin;
                    let elapsed_time_ms = elapsed_time.as_millis();
                    if elapsed_time_ms >= 60000 {
                        running = false;
                        break;
                    }
                }
            }
        }
        let end = Instant::now();
        let elapsed_time_ms = (end - begin).as_millis();
        println!("query count: {}", cnt);
        println!("time cost: {} ms", elapsed_time_ms);
        println!("qps: {}", cnt /(elapsed_time_ms as u64));
        Ok(())
    }
}
