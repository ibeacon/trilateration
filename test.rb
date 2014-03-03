#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require 'yaml'

client = Elasticsearch::Client.new host: 'data.ibeacon.cz:9200', log: false

client.transport.reload_connections!

client.cluster.health

# client.search q: 'estymote_region_state.hits'

# client.get index: 'my_app', type: 'blog_post', id: 1

DEVICES = {
  iphone1: "8344B4A4-51FC-4140-B7E1-D8CB1D777567",
  iphone2: "DE81833A-F1FC-4B01-94A8-5F6D9B4F395D",
  iphone3: "F1D4F6B3-E649-454C-99F0-3AAFE7650558",
  iphone4: "6B747116-AB6E-41CC-B465-385F894C031B",
  iphone5: "B058B9C2-1CA4-49FB-A281-EE5D9926573D",
}
BIGNUMBER = 15000
# SET = "estymote_beacons"
SET = "estymote_region_state"
DISTANCE_COEF = 1.0
BEACONS = {
  "15200" => [14310, 9150],
  "27111" => [23250, 7950],
  "37219" => [15360, 16350],
  "52354" => [4410, 8210],
  "49277" => [15600,250],
  "22689" => [8750,11800],
  "49846" => [9050,2610],
  "28389" => [1720, 12750],
  "9403" => [2900,250]
}

def get_coordinates(a, b, c, da, db, dc) # [x, y] / float
  w, z, x, y, y2 = 0.0
  
  w = da*da - db*db - a[0]*a[0] - a[1]*a[1] + b[0]*b[0] + b[1]*b[1];
  z = db*db - dc*dc - b[0]*b[0] - b[1]*b[1] + c[0]*c[0] + c[1]*c[1];
  
  x = (w*(c[1]-b[1]) - z*(b[1]-a[1])) / (2 * ((b[0]-a[0])*(c[1]-b[1]) - (c[0]-b[0])*(b[1]-a[1])));
  y = (w - 2*x*(b[0]-a[0])) / (2*(b[1]-a[1]));
  
  #y2 is a second measure of y to mitigate errors
  
  y2 = (z - 2*x*(c[0]-b[0])) / (2*(c[1]-b[1]));

  y = (y + y2) / 2;
  return ([x.infinite?, x.nan?, y.infinite?, y.nan?].any?) ? nil : [x, y];
end

def weight_avg(value1, weight1, value2, weight2)
  (weight1 * value1) + (weight2 * value2) / (weight1.to_f + weight2)
end

def get_coordinates_two(a, b, da, db)
  xa, ya = a
  xb, yb = b

  return [weight_avg(xa, 1/da, xb, 1/db),
          weight_avg(ya, 1/da, yb, 1/db)]
end

def beacon_coords_for_beacon(beacon)
  major = beacon['estymote_beacon']['major'].to_s
  BEACONS[major]
end

def beacon_distance_for_beacon(beacon)
  major = beacon['estymote_beacon']['distance'].to_f
end


# Search:
results = client.search(q: "device.identifier_for_vendor:#{DEVICES[:iphone1]} AND type:estymote_beacons", size: BIGNUMBER)["hits"]["hits"]

distribution = {}
distribution.default = 0

for result in results
    timestamp = result["_source"]["timestamp"]
    time_utc = Time.at(timestamp).utc
    #time_norm = timestamp.to_i/100*100
    device_id = result["_source"]["device"]["identifier_for_vendor"]
    
    beacons_raw = result["_source"]["beacons"]
    unless beacons_raw.nil?
      
      # puts "--- #{beacons_raw.size}"
      
      # bmax = beacons_raw.map do |beacon|
      #   distance_ok = beacon_distance_for_beacon(beacon) >= 0.0
      #   puts "#{distance_ok}"
      #   distance_ok ? beacon : nil
      # end
      
      bmax = beacons_raw.reject do |beacon|
        beacon_distance_for_beacon(beacon) < 0
      end.sort do |a,b|
        beacon_distance_for_beacon(a) <=> beacon_distance_for_beacon(b)
      end[0..2]

      distribution[bmax.size] += 1
      
      # puts "#{beacons_raw.size} - #{bmax}"
      #.compact.sort do |a,b|
        # beacon_distance_for_beacon(a) <=> beacon_distance_for_beacon(b)
      # end[0..2]
      
      coordinates = nil

      if !bmax.empty? && bmax.size==3
        beacon_a = beacon_coords_for_beacon bmax[0]
        beacon_b = beacon_coords_for_beacon bmax[1]
        beacon_c = beacon_coords_for_beacon bmax[2]
        dis_a = beacon_distance_for_beacon bmax[0]
        dis_b = beacon_distance_for_beacon bmax[1]
        dis_c = beacon_distance_for_beacon bmax[2]
        # puts [beacon_a, beacon_b, beacon_c, dis_a, dis_b, dis_c].join(" / ")
        if [beacon_a, beacon_b, beacon_c, dis_a, dis_b, dis_c].all?
          # puts "#{[beacon_a, beacon_b, beacon_c, dis_a, dis_b, dis_c].to_yaml}"
          
          coordinates = get_coordinates(beacon_a, beacon_b, beacon_c, dis_a, dis_b, dis_c)
        end
      elsif bmax.size == 2
        beacon_a = beacon_coords_for_beacon bmax[0]
        beacon_b = beacon_coords_for_beacon bmax[1]
        dist_a = beacon_distance_for_beacon bmax[0]
        dist_b = beacon_distance_for_beacon bmax[1]

        if [beacon_a, beacon_b, dist_a, dist_b].all?
          coordinates = get_coordinates_two(beacon_a, beacon_b, dist_a, dist_b)
        end
      else
        coordinates = [:null, :null]
      end
      if coordinates
        puts "#{device_id},#{timestamp},#{coordinates[0]},#{coordinates[1]}" if coordinates
      end
  end
end

$stderr.puts "distribution: #{distribution}"

