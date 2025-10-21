from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time
import threading

class StreamMonitor:
    def __init__(self, port=8000):
        # Start Prometheus metrics server
        start_http_server(port)
        
        # Define metrics for monitoring
        self.messages_processed = Counter(
            'stream_messages_processed_total', 
            'Total number of processed messages'
        )
        
        self.processing_latency = Histogram(
            'stream_processing_latency_seconds', 
            'Processing latency in seconds',
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
        )
        
        self.dropped_messages = Counter(
            'stream_messages_dropped_total', 
            'Total number of dropped messages'
        )
        
        self.throughput = Gauge(
            'stream_throughput_messages_per_second', 
            'Current throughput in messages per second'
        )
        
        # Start throughput calculator thread
        self.last_count = 0
        self.throughput_thread = threading.Thread(target=self._calculate_throughput)
        self.throughput_thread.daemon = True
        self.throughput_thread.start()