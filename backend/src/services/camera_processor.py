"""
Camera processor service for managing RTSP streams and person detection.
Handles auto-starting processors when cameras are added.
"""

import os
import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
import subprocess
import signal
from pathlib import Path

logger = logging.getLogger(__name__)

class CameraProcessorManager:
    def __init__(self):
        self.processors: Dict[str, Dict[str, Any]] = {}
        self.base_dir = Path(os.getenv("PROCESSOR_DIR", "processors"))
        self.base_dir.mkdir(exist_ok=True)
        
    async def start_processor(self, camera_id: str, rtsp_url: str, store_id: str) -> bool:
        """Start a camera processor for the given camera."""
        try:
            # Stop existing processor if running
            await self.stop_processor(camera_id)
            
            # Create processor directory
            processor_dir = self.base_dir / camera_id
            processor_dir.mkdir(exist_ok=True)
            
            # Prepare processor configuration
            config = {
                "camera_id": camera_id,
                "store_id": store_id,
                "rtsp_url": rtsp_url,
                "output_dir": str(processor_dir),
                "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379"),
                "database_url": os.getenv("DATABASE_URL", ""),
                "model_path": os.getenv("YOLO_MODEL_PATH", "models/yolov8n.pt"),
                "enable_reid": os.getenv("ENABLE_REID", "true").lower() == "true",
                "detection_interval": float(os.getenv("DETECTION_INTERVAL", "0.1")),
                "heartbeat_interval": int(os.getenv("HEARTBEAT_INTERVAL", "30"))
            }
            
            # Save configuration
            config_path = processor_dir / "config.json"
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Start processor using Docker or subprocess
            if os.getenv("USE_DOCKER_PROCESSOR", "false").lower() == "true":
                process = await self._start_docker_processor(camera_id, config_path)
            else:
                process = await self._start_subprocess_processor(camera_id, config_path)
            
            if process:
                self.processors[camera_id] = {
                    "process": process,
                    "config": config,
                    "started_at": datetime.utcnow(),
                    "status": "running"
                }
                
                logger.info(f"Started processor for camera {camera_id}")
                return True
            else:
                logger.error(f"Failed to start processor for camera {camera_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error starting processor for camera {camera_id}: {e}")
            return False
    
    async def _start_docker_processor(self, camera_id: str, config_path: Path) -> Optional[subprocess.Popen]:
        """Start processor using Docker."""
        try:
            cmd = [
                "docker", "run", "-d",
                "--name", f"wink-processor-{camera_id}",
                "-v", f"{config_path.parent}:/app/data",
                "-v", f"{config_path}:/app/config.json",
                "--network", "host",
                "wink-processor:latest"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                container_id = result.stdout.strip()
                # Return a mock process object for Docker containers
                class DockerProcess:
                    def __init__(self, container_id):
                        self.container_id = container_id
                        self.pid = None
                        self.returncode = None
                    
                    def poll(self):
                        # Check if container is running
                        result = subprocess.run(
                            ["docker", "ps", "-q", "-f", f"id={self.container_id}"],
                            capture_output=True, text=True
                        )
                        return None if result.stdout.strip() else 0
                    
                    def terminate(self):
                        subprocess.run(["docker", "stop", self.container_id])
                    
                    def kill(self):
                        subprocess.run(["docker", "kill", self.container_id])
                
                return DockerProcess(container_id)
            else:
                logger.error(f"Docker container failed to start: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error starting Docker processor: {e}")
            return None
    
    async def _start_subprocess_processor(self, camera_id: str, config_path: Path) -> Optional[subprocess.Popen]:
        """Start processor using subprocess."""
        try:
            # Use the processor script
            processor_script = Path(__file__).parent / "processor_worker.py"
            
            cmd = [
                "python", str(processor_script),
                "--config", str(config_path),
                "--camera-id", camera_id
            ]
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=config_path.parent
            )
            
            # Give it a moment to start
            await asyncio.sleep(1)
            
            # Check if process is still running
            if process.poll() is None:
                return process
            else:
                logger.error(f"Processor failed to start for camera {camera_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error starting subprocess processor: {e}")
            return None
    
    async def stop_processor(self, camera_id: str) -> bool:
        """Stop a camera processor."""
        try:
            processor_info = self.processors.get(camera_id)
            
            if not processor_info:
                logger.info(f"No processor running for camera {camera_id}")
                return True
            
            process = processor_info["process"]
            
            # Terminate the process gracefully
            if hasattr(process, 'terminate'):
                process.terminate()
                
                # Wait for graceful shutdown
                try:
                    await asyncio.wait_for(self._wait_for_process_end(process), timeout=10)
                except asyncio.TimeoutError:
                    # Force kill if it doesn't terminate gracefully
                    if hasattr(process, 'kill'):
                        process.kill()
                        await asyncio.wait_for(self._wait_for_process_end(process), timeout=5)
            
            # Clean up Docker container if using Docker
            if hasattr(process, 'container_id'):
                subprocess.run(["docker", "rm", "-f", process.container_id], 
                             capture_output=True)
            
            # Remove from tracking
            del self.processors[camera_id]
            
            logger.info(f"Stopped processor for camera {camera_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping processor for camera {camera_id}: {e}")
            return False
    
    async def _wait_for_process_end(self, process):
        """Wait for process to end."""
        while process.poll() is None:
            await asyncio.sleep(0.1)
    
    def get_processor_status(self, camera_id: str) -> Dict[str, Any]:
        """Get status of a camera processor."""
        processor_info = self.processors.get(camera_id)
        
        if not processor_info:
            return {
                "running": False,
                "status": "not_running"
            }
        
        process = processor_info["process"]
        is_running = process.poll() is None
        
        status = {
            "running": is_running,
            "status": "running" if is_running else "stopped",
            "started_at": processor_info["started_at"].isoformat(),
            "config": processor_info["config"],
            "pid": getattr(process, 'pid', None)
        }
        
        if not is_running:
            status["exit_code"] = process.returncode
        
        return status
    
    def list_processors(self) -> Dict[str, Dict[str, Any]]:
        """List all running processors."""
        return {
            camera_id: self.get_processor_status(camera_id)
            for camera_id in self.processors.keys()
        }
    
    async def cleanup_all_processors(self):
        """Stop all running processors."""
        camera_ids = list(self.processors.keys())
        
        for camera_id in camera_ids:
            await self.stop_processor(camera_id)
        
        logger.info("All processors stopped")

# Global processor manager
processor_manager = CameraProcessorManager()

# Public interface functions
async def start_camera_processor(camera_id: str, rtsp_url: str, store_id: str) -> bool:
    """Start a camera processor."""
    return await processor_manager.start_processor(camera_id, rtsp_url, store_id)

async def stop_camera_processor(camera_id: str) -> bool:
    """Stop a camera processor."""
    return await processor_manager.stop_processor(camera_id)

def get_camera_status(camera_id: str) -> Dict[str, Any]:
    """Get camera processor status."""
    return processor_manager.get_processor_status(camera_id)

def list_all_processors() -> Dict[str, Dict[str, Any]]:
    """List all processor statuses."""
    return processor_manager.list_processors()

async def cleanup_processors():
    """Stop all processors (for app shutdown)."""
    await processor_manager.cleanup_all_processors()