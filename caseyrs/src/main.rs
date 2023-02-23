#![feature(type_alias_impl_trait)]
use serde_json;
use std::fs::File;
use serde::{Serialize,Deserialize};
use std::time::{Instant};
use std::io::{BufReader,BufRead};
use std::io::{Read,Seek};
use rayon::iter::{IntoParallelIterator,ParallelIterator,ParallelBridge};

#[derive(Serialize, Deserialize, Debug)]
struct Dist {
    x0: f64,
    x1: f64,
    y0: f64,
    y1: f64,
}

#[derive(Serialize, Deserialize)]
struct JSON { pairs: Vec<Dist> }

fn consume<const N: usize>(r: &mut (impl Read + Seek + BufRead), s: &[u8]) {
    let mut buf = [0; N];
    r.read_exact(&mut buf).unwrap();
    if buf.as_slice() != s {
        panic!("{}", format!("{:?} != {:?}", buf, s));
    }
}

fn consume_float(r: &mut (impl Read + Seek + BufRead)) -> f64 {
    let mut i = 0;
    let mut buf = [0u8; 30];

    loop {
        let peek = r.fill_buf().unwrap();
        if peek[0] == b',' || peek[0] == b'}' {
            break;
        }
        r.read_exact(&mut buf[i..i+1]).unwrap();
        i += 1;
    }
    
    let s = unsafe { std::str::from_utf8_unchecked(&buf[0..i]) };
    s.parse().unwrap()
}

// TODO: handle whitespace
fn maybe_consume_dist(r: &mut (impl Read + Seek + BufRead)) -> Option<Dist> {
    let b = r.fill_buf().unwrap();
    match b[0] {
        b',' => r.consume(1),
        b']' => return None,
        b'{' => (),
        _ => panic!("unexpected: {:?}", b[0] as char),
    }

    let mut dist = Dist { x0: 0., x1: 0., y0: 0., y1: 0. };
    consume::<1>(r, b"{");
    let mut keybuf = [0u8; 2];

    for _ in 0..3 {
        consume_key(r, &mut keybuf);
        let value = consume_float(r);
        match &keybuf {
            b"x0" => dist.x0 = value,
            b"x1" => dist.x1 = value,
            b"y0" => dist.y0 = value,
            b"y1" => dist.y1 = value,
            _ => panic!("didn't expect key {:?}", keybuf),
        }
        consume::<1>(r, b",");
    }
    consume_key(r, &mut keybuf);
    let value = consume_float(r);
    match &keybuf {
        b"x0" => dist.x0 = value,
        b"x1" => dist.x1 = value,
        b"y0" => dist.y0 = value,
        b"y1" => dist.y1 = value,
        _ => panic!("didn't expect key {:?}", keybuf),
    }
    consume::<1>(r, b"}");
    Some(dist)
}

fn consume_key(r: &mut (impl Read + Seek + BufRead), buf: &mut [u8]) {
    consume::<1>(r, b"\"");
    r.read_exact(buf).unwrap();
    consume::<2>(r, b"\":");
}

fn parse_json(r: &mut (impl Read + Seek + BufRead)) -> impl Iterator<Item = Dist> + '_ {
    const PRELUDE: &[u8] = b"{\"pairs\": [";
    consume::<{ PRELUDE.len() }>(r, PRELUDE); 
    std::iter::from_fn(move || maybe_consume_dist(r))
}

fn main() {
    let start_time = Instant::now();
    // 
    // Read the input
    // 
    let f = File::open("../data_10000000_flex.json").unwrap();
    let mut f = BufReader::new(f);
    let dists = parse_json(&mut f);

    let mid_time = Instant::now();

    //
    // Average the haversines
    //
    fn haversine_of_degrees(x0: f64, y0: f64, x1: f64, y1:f64, r: f64) -> f64 {
      let dy = (y1 - y0).to_radians();
      let dx = (x1 - x0).to_radians();
      let y0 = y0.to_radians();
      let y1 = y1.to_radians();

      let root_term = (dy/2.).sin().powf(2.) + y0.cos()*y1.cos()*(dx/2.).sin().powf(2.);
      2. * r * root_term.sqrt().asin()
    }

    const EARTH_RADIUS_KM : f64 = 6371.;
    let (sum, count) : (f64, usize) = dists.par_bridge()
        .map(|pair| (haversine_of_degrees(pair.x0, pair.y0, pair.x1, pair.y1, EARTH_RADIUS_KM), 1))
        .reduce(|| (0., 0), |acc, this| (acc.0 + this.0, acc.1 + this.1));

    let average = sum / count as f64;
    let end_time = Instant::now();

    //
    // Display the result
    //
    println!("Result: {average}");
    let input = (mid_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Input = {input} seconds");
    let math = (end_time - mid_time).as_nanos() as f64 / 1.0e9;
    println!("Math = {math} seconds");
    let total = (end_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Total = {total} seconds");
    let throughput = count as f64 / total;
    println!("Throughput = {throughput} haversines/second");
}

#[allow(unused)]
fn main_rayon() {
    let start_time = Instant::now();
    // 
    // Read the input
    // 
    let f = File::open("../data_10000000_flex.json").unwrap();
    let f = BufReader::new(f);
    let j : JSON = serde_json::from_reader(f).unwrap();
    let pairs = j.pairs;

    let mid_time = Instant::now();

    //
    // Average the haversines
    //
    fn haversine_of_degrees(x0: f64, y0: f64, x1: f64, y1:f64, r: f64) -> f64 {
      let dy = (y1 - y0).to_radians();
      let dx = (x1 - x0).to_radians();
      let y0 = y0.to_radians();
      let y1 = y1.to_radians();

      let root_term = (dy/2.).sin().powf(2.) + y0.cos()*y1.cos()*(dx/2.).sin().powf(2.);
      2. * r * root_term.sqrt().asin()
    }

    const EARTH_RADIUS_KM : f64 = 6371.;
    let count = pairs.len();
    let sum : f64 = pairs.into_par_iter()
        .map(|pair| haversine_of_degrees(pair.x0, pair.y0, pair.x1, pair.y1, EARTH_RADIUS_KM))
        .reduce(|| 0., |acc, n| acc + n);

    let average = sum / count as f64;
    let end_time = Instant::now();

    //
    // Display the result
    //
    println!("Result: {average}");
    let input = (mid_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Input = {input} seconds");
    let math = (end_time - mid_time).as_nanos() as f64 / 1.0e9;
    println!("Math = {math} seconds");
    let total = (end_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Total = {total} seconds");
    let throughput = count as f64 / total;
    println!("Throughput = {throughput} haversines/second");
}

#[allow(unused)]
fn main_naive() {
    let start_time = Instant::now();
    // 
    // Read the input
    // 
    let f = File::open("../data_10000000_flex.json").unwrap();
    let f = BufReader::new(f);
    let j : JSON = serde_json::from_reader(f).unwrap();
    let pairs = j.pairs;

    let mid_time = Instant::now();

    //
    // Average the haversines
    //
    fn haversine_of_degrees(x0: f64, y0: f64, x1: f64, y1:f64, r: f64) -> f64 {
      let dy = (y1 - y0).to_radians();
      let dx = (x1 - x0).to_radians();
      let y0 = y0.to_radians();
      let y1 = y1.to_radians();

      let root_term = (dy/2.).sin().powf(2.) + y0.cos()*y1.cos()*(dx/2.).sin().powf(2.);
      2. * r * root_term.sqrt().asin()
    }

    const EARTH_RADIUS_KM : f64 = 6371.;
    let mut sum = 0.;
    let mut count = 0;

    for pair in pairs {
        sum += haversine_of_degrees(pair.x0, pair.y0, pair.x1, pair.y1, EARTH_RADIUS_KM);
        count += 1;
    }

    let average = sum / count as f64;
    let end_time = Instant::now();

    //
    // Display the result
    //
    println!("Result: {average}");
    let input = (mid_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Input = {input} seconds");
    let math = (end_time - mid_time).as_nanos() as f64 / 1.0e9;
    println!("Math = {math} seconds");
    let total = (end_time - start_time).as_nanos() as f64 / 1.0e9;
    println!("Total = {total} seconds");
    let throughput = count as f64 / total;
    println!("Throughput = {throughput} haversines/second");
}
