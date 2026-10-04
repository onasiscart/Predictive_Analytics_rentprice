import json
import random
import statistics
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

def calculate_trend(values: List[float]) -> float:
    """
    Calculates the average arithmetic difference between consecutive elements 
    in a numeric list to determine the historical trend.
    """
    if len(values) < 2:
        return 0.0
    
    diffs = [values[i] - values[i-1] for i in range(1, len(values))]
    return float(statistics.mean(diffs))

def add_random_variation(value: float, variance: float = 0.15) -> float:
    """
    Applies a stochastic variation to a given base value based on 
    a specified variance percentage.
    """
    variation = value * variance * (random.random() - 0.5)
    return round(value + variation, 1)

def generate_synthetic_data(input_file: str, output_file: str, end_year: int = 2025) -> None:
    """
    Extends historical neighborhood datasets by appending synthetic projections 
    up to a target year. It preserves existing data and calculates future values 
    using a combination of linear trends and random noise.
    """
    
    # Read input data from newline-delimited JSON
    records: List[Dict[str, Any]] = []
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    
    print(f"Loaded {len(records)} records from source.")
    
    updated_records: List[Dict[str, Any]] = []
    for record in records:
        # Create a deep copy of the historical info list
        info: List[Dict[str, Any]] = [entry.copy() for entry in record['info']]
        last_year = info[-1]['year']
        
        neighborhood = record.get('neigh_name ', 'Unknown')
        print(f"Extending data for {neighborhood}: from {last_year} to {end_year}")
        
        # Extract historical metrics for trend estimation
        amounts = [entry.get('Amount', 0.0) for entry in info]
        used_amounts = [entry.get('usedAmount', 0.0) for entry in info]
        per_meters = [entry.get('PerMeter', 0.0) for entry in info]
        used_per_meters = [entry.get('usedPerMeter', 0.0) for entry in info]
        
        # Calculate individual trends for each metric
        amount_trend = calculate_trend(amounts)
        used_amount_trend = calculate_trend(used_amounts)
        per_meter_trend = calculate_trend(per_meters)
        used_per_meter_trend = calculate_trend(used_per_meters)
        
        # Iteratively generate future years
        for year in range(last_year + 1, end_year + 1):
            prev_entry = info[-1]
            
            # Retrieve previous values or fall back to last known historical value
            prev_amount = prev_entry.get('Amount', amounts[-1] if amounts else 0.0)
            prev_used_amount = prev_entry.get('usedAmount', used_amounts[-1] if used_amounts else 0.0)
            prev_per_meter = prev_entry.get('PerMeter', per_meters[-1] if per_meters else 0.0)
            prev_used_per_meter = prev_entry.get('usedPerMeter', used_per_meters[-1] if used_per_meters else 0.0)
            
            # Compute new values ensuring they remain non-negative
            new_amount = add_random_variation(prev_amount + amount_trend)
            new_used_amount = add_random_variation(prev_used_amount + used_amount_trend)
            new_per_meter = add_random_variation(prev_per_meter + per_meter_trend)
            new_used_per_meter = add_random_variation(prev_used_per_meter + used_per_meter_trend)
            
            new_entry = {
                'year': year,
                'Amount': max(0.0, new_amount),
                'usedAmount': max(0.0, new_used_amount),
                'PerMeter': max(0.0, new_per_meter),
                'usedPerMeter': max(0.0, new_used_per_meter)
            }
            
            # Conditionally add secondary metrics (70% probability)
            if random.random() > 0.3:
                new_entry['diffAmount'] = round(add_random_variation(new_amount * 1.1, 0.2), 1)
                new_entry['diffPerMeter'] = round(add_random_variation(new_per_meter * 1.1, 0.2), 1)
            
            info.append(new_entry)
        
        # Update record with the extended time series
        updated_record = record.copy()
        updated_record['info'] = info
        updated_records.append(updated_record)
    
    # Save results to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in updated_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    print(f"Process completed. Data available until {end_year}.")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    # Internal configuration for data paths
    INPUT_FILE = "input_data.json"
    OUTPUT_FILE = "output_data_with_synthetic.json"
    
    generate_synthetic_data(INPUT_FILE, OUTPUT_FILE)