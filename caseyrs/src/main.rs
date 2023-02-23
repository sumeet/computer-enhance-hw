use serde_json;
use std::fs::File;
use serde::{Serialize,Deserialize};
use std::time::{Instant};
use std::io::BufReader;
use rayon::iter::{IntoParallelIterator,ParallelIterator};

#[derive(Serialize, Deserialize)]
struct Dist {
    x0: f64,
    x1: f64,
    y0: f64,
    y1: f64,
}

#[derive(Serialize, Deserialize)]
struct JSON { pairs: Vec<Dist> }

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

// using rayon
fn main() {
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
