use serde_json;
use std::fs::File;
use serde::{Serialize,Deserialize};
use std::time::{Instant};
use std::io::BufReader;

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

/*
JSONFile = open('data_10000000_flex.json')


Sum = 0
Count = 0
for Pair in JSONInput['pairs']: 
    Sum += HaversineOfDegrees(Pair['x0'], Pair['y0'], Pair['x1'], Pair['y1'], EarthRadiuskm)
    Count += 1
Average = Sum / Count
EndTime = time.time()

#
# Display the result
#

print("Result: " + str(Average))
print("Input = " + str(MidTime - StartTime) + " seconds")
print("Math = " + str(EndTime - MidTime) + " seconds")
print("Total = " + str(EndTime - StartTime) + " seconds")
print("Throughput = " + str(Count/(EndTime - StartTime)) + " haversines/second")
*/
