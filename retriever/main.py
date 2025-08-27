from maneger import PipelineManager
import time

if __name__ == '__main__':
    while True:
        manager = PipelineManager()
        processed_count = manager.run_pipeline(target_column="Antisemitic")
        print(f"Processed {processed_count} tweets.")
        time.sleep(60)