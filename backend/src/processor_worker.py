#!/usr/bin/env python3
"""
Camera processor worker entry point.
This service processes camera feeds and performs AI detection.
"""

import asyncio
import signal
import sys
import time
import logging
import os
from typing import Optional
from src.camera.processor import run_camera

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessorWorker:
    def __init__(self):
        self.running = False
        
    async def start(self):
        """Start the processor worker."""
        logger.info("Starting camera processor worker...")
        self.running = True
        
        try:
            # For now, just log that the worker is running
            # In a real implementation, this would connect to a camera
            # and process frames using the run_camera function
            
            camera_id = int(os.getenv("CAMERA_ID", "1"))
            rtsp_url = os.getenv("RTSP_URL", "")
            
            if rtsp_url:
                logger.info(f"Processing camera {camera_id} with URL: {rtsp_url}")
                run_camera(camera_id, rtsp_url)
            else:
                logger.info("No RTSP URL configured, running in idle mode...")
                while self.running:
                    await asyncio.sleep(10)  # Keep worker alive
                    
        except Exception as e:
            logger.error(f"Error in processor worker: {e}")
            raise
        finally:
            logger.info("Camera processor worker stopped")
            
    def stop(self):
        """Stop the processor worker."""
        logger.info("Stopping camera processor worker...")
        self.running = False

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    worker.stop()
    sys.exit(0)

# Global worker instance
worker = ProcessorWorker()

async def main():
    """Main entry point."""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        worker.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())