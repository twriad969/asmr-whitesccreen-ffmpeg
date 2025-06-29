import os
import uuid
import asyncio
import subprocess
import json
import traceback
from pathlib import Path
from typing import Optional, Dict, Any
import aiofiles
import aiohttp
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Global variables
job_manager = None

# Configuration
MAX_CONCURRENT_JOBS = 5
UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("outputs")
TEMP_DIR = Path("temp")

# Create directories
for dir_path in [UPLOAD_DIR, OUTPUT_DIR, TEMP_DIR]:
    dir_path.mkdir(exist_ok=True)

# Job tracking
jobs: Dict[str, Dict[str, Any]] = {}
active_jobs = 0
job_queue = asyncio.Queue()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

class JobManager:
    def __init__(self):
        self.running = True
    
    async def process_queue(self):
        while self.running:
            try:
                job_id = await asyncio.wait_for(job_queue.get(), timeout=1.0)
                asyncio.create_task(self.process_job(job_id))
            except asyncio.TimeoutError:
                continue
    
    async def process_job(self, job_id: str):
        global active_jobs
        async with semaphore:
            active_jobs += 1
            try:
                await self.run_ffmpeg(job_id)
            finally:
                active_jobs -= 1
    
    async def run_ffmpeg(self, job_id: str):
        job = jobs[job_id]
        image_path = job['image_path']
        video_path = job['video_path']
        output_path = job['output_path']
        
        try:
            jobs[job_id]['status'] = 'processing'
            jobs[job_id]['progress'] = 0
            
            cmd = [
                'ffmpeg', '-y',
                '-loop', '1', '-i', str(image_path),
                '-i', str(video_path),
                '-map_metadata', '1',
                '-map_chapters', '1',
                '-movflags', 'use_metadata_tags',
                '-filter_complex',
                '[0:v]scale=ih*9/16:ih[bg];[1:v]scale=iw*1.45:-1,format=rgba,drawbox=0:0:iw:50:color=black@0.0:t=fill,drawbox=0:ih-50:iw:50:color=black@0.0:t=fill,drawbox=0:0:50:ih:color=black@0.0:t=fill,drawbox=iw-50:0:50:ih:color=black@0.0:t=fill[vid];[bg][vid]overlay=(W-w)/2:(H-h)/2:format=auto',
                '-shortest',
                '-pix_fmt', 'yuv420p',
                '-c:v', 'libx264',
                '-crf', '18',
                '-preset', 'veryfast',
                '-progress', 'pipe:1',
                str(output_path)
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            duration = await self.get_video_duration(video_path)
            
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                
                line = line.decode().strip()
                if line.startswith('out_time='):
                    time_str = line.split('=')[1]
                    current_time = self.parse_time(time_str)
                    if duration > 0:
                        progress = min(int((current_time / duration) * 100), 99)
                        jobs[job_id]['progress'] = progress
            
            await process.wait()
            
            if process.returncode == 0:
                jobs[job_id]['status'] = 'completed'
                jobs[job_id]['progress'] = 100
                jobs[job_id]['download_url'] = f"{os.getenv('API_URL', 'http://localhost:8000')}/download/{job_id}"
            else:
                stderr = await process.stderr.read()
                jobs[job_id]['status'] = 'failed'
                jobs[job_id]['error'] = stderr.decode()
                
        except Exception as e:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = str(e)
    
    async def get_video_duration(self, video_path: str) -> float:
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', str(video_path)
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE
            )
            stdout, _ = await process.communicate()
            data = json.loads(stdout.decode())
            return float(data['format']['duration'])
        except:
            return 0
    
    def parse_time(self, time_str: str) -> float:
        try:
            parts = time_str.split(':')
            if len(parts) == 3:
                h, m, s = parts
                return float(h) * 3600 + float(m) * 60 + float(s)
            return float(time_str)
        except:
            return 0

job_manager = JobManager()

@app.on_event("startup")
async def startup_event():
    global job_manager
    job_manager = JobManager()
    asyncio.create_task(job_manager.process_queue())

async def download_file(url: str, file_path: Path) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    return True
                return False
    except:
        return False

@app.post("/process")
async def process_video(
    background_tasks: BackgroundTasks,
    image: Optional[UploadFile] = File(None),
    video: Optional[UploadFile] = File(None),
    image_url: Optional[str] = Form(None),
    video_url: Optional[str] = Form(None)
):
    try:
        if not ((image or image_url) and (video or video_url)):
            raise HTTPException(status_code=400, detail="Provide either files or URLs for both image and video")
        
        job_id = str(uuid.uuid4())
        image_path = UPLOAD_DIR / f"{job_id}_image"
        video_path = UPLOAD_DIR / f"{job_id}_video"
        output_path = OUTPUT_DIR / f"{job_id}_output.mp4"
        
        jobs[job_id] = {
            'status': 'uploading',
            'progress': 0,
            'image_path': image_path,
            'video_path': video_path,
            'output_path': output_path,
            'created_at': asyncio.get_event_loop().time()
        }
        
        # Handle image
        if image:
            if not image.content_type or not any(x in image.content_type for x in ['image/jpeg', 'image/jpg', 'image/png']):
                # Try to determine from filename
                if not image.filename or not any(image.filename.lower().endswith(x) for x in ['.jpg', '.jpeg', '.png']):
                    raise HTTPException(status_code=400, detail="Image must be JPEG or PNG")
            
            ext = '.png' if (image.filename and image.filename.lower().endswith('.png')) else '.jpg'
            
            image_path = image_path.with_suffix(ext)
            jobs[job_id]['image_path'] = image_path
            
            async with aiofiles.open(image_path, 'wb') as f:
                content = await image.read()
                await f.write(content)
        else:
            ext = '.png' if '.png' in image_url.lower() else '.jpg'
            image_path = image_path.with_suffix(ext)
            jobs[job_id]['image_path'] = image_path
            
            if not await download_file(image_url, image_path):
                jobs[job_id]['status'] = 'failed'
                jobs[job_id]['error'] = 'Failed to download image'
                raise HTTPException(status_code=400, detail="Failed to download image")
        
        # Handle video
        if video:
            ext = '.mkv' if (video.filename and '.mkv' in video.filename.lower()) else '.mp4'
            video_path = video_path.with_suffix(ext)
            jobs[job_id]['video_path'] = video_path
            
            async with aiofiles.open(video_path, 'wb') as f:
                content = await video.read()
                await f.write(content)
        else:
            ext = '.mkv' if '.mkv' in video_url.lower() else '.mp4'
            video_path = video_path.with_suffix(ext)
            jobs[job_id]['video_path'] = video_path
            
            if not await download_file(video_url, video_path):
                jobs[job_id]['status'] = 'failed'
                jobs[job_id]['error'] = 'Failed to download video'
                raise HTTPException(status_code=400, detail="Failed to download video")
        
        jobs[job_id]['status'] = 'queued'
        await job_queue.put(job_id)
        
        return {
            "job_id": job_id,
            "progress_url": f"{os.getenv('API_URL', 'http://localhost:8000')}/progress/{job_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in process_video: {str(e)}")
        print(traceback.format_exc())
        if 'job_id' in locals():
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = str(e)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/progress/{job_id}")
async def get_progress(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    response = {
        "job_id": job_id,
        "status": job['status'],
        "progress": job['progress']
    }
    
    if job['status'] == 'completed':
        response['download_url'] = job.get('download_url')
    elif job['status'] == 'failed':
        response['error'] = job.get('error')
    elif job['status'] == 'queued':
        response['queue_position'] = await get_queue_position(job_id)
        response['active_jobs'] = active_jobs
    
    return response

async def get_queue_position(job_id: str) -> int:
    # Simple queue position estimation
    position = 0
    for jid, job in jobs.items():
        if job['status'] == 'queued' and job['created_at'] < jobs[job_id]['created_at']:
            position += 1
    return position

@app.get("/download/{job_id}")
async def download_file_endpoint(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Job not completed")
    
    output_path = job['output_path']
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    
    return FileResponse(
        path=str(output_path),
        filename=f"processed_{job_id}.mp4",
        media_type="video/mp4"
    )