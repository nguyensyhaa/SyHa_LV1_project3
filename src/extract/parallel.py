import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from config.settings import CHUNK_SIZE, CPU_CORES
import traceback
from src.extract.generator import generate_products

def generate_data_parallel(total_records: int) -> list:
    """
    Generates data using multiple CPU cores.
    Splits total_records into chunks and processes them in parallel.
    """
    # Calculate how many chunks we need
    num_chunks = total_records // CHUNK_SIZE
    remainder = total_records % CHUNK_SIZE
    
    tasks = [CHUNK_SIZE] * num_chunks
    if remainder > 0:
        tasks.append(remainder)
        
    results = []
    
    print(f"Starting generation of {total_records} rows using {CPU_CORES} cores...")
    
    with ProcessPoolExecutor(max_workers=CPU_CORES) as executor:
        # Map tasks to the generator function
        futures = {executor.submit(generate_products, count): count for count in tasks}
        
        for future in as_completed(futures):
            try:
                chunk = future.result()
                results.extend(chunk)
            except Exception as e:
                print(f"Chunk generation failed: {e}")
                traceback.print_exc()
                
    print(f"Generation complete. Total rows: {len(results)}")
    return results
