import threading
import asyncio
import trio
import queue
import logging
from typing import Any, Callable, Awaitable

logger = logging.getLogger("trio-bridge")

class TrioBridge:
    def __init__(self):
        self._request_queue = queue.Queue()
        self._thread = None
        self._loop = None # Asyncio loop
        self._running = False
        self.host = None # Expected to be set by the trio loop
        self.nursery = None 

    def start(self):
        if self._running:
            return
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(target=trio.run, args=(self._trio_main,), daemon=True)
        self._thread.start()
        logger.info("Trio background thread started")

    async def _trio_main(self):
        """Main loop running in Trio thread."""
        logger.info("Trio loop running")
        
        async with trio.open_nursery() as nursery:
            self.nursery = nursery
            
            while self._running:
                try:
                    # Non-blocking check of queue to allow sleep
                    # Ideally use a trio-friendly synchronization, but polling is safe fallback
                    try:
                        req = self._request_queue.get_nowait()
                    except queue.Empty:
                        await trio.sleep(0.01)
                        continue

                    func, args, fut = req
                    
                    # Execute the trio-compatible function
                    # We wrap execution in a nursery task to isolate failures and context? 
                    # No, we need result. We run it inline.
                    
                    try:
                        if asyncio.iscoroutinefunction(func) or hasattr(func, 'asend'):
                            # It's a trio async function - await it
                            # BUT we can't await a func object directly if passed as partial
                            # We assume func is an async function and we call it with args
                            res = await func(*args)
                        else:
                            res = func(*args)
                        
                        # Send result back to asyncio
                        if self._loop and not fut.done():
                             self._loop.call_soon_threadsafe(fut.set_result, res)
                    except Exception as e:
                        if self._loop and not fut.done():
                             self._loop.call_soon_threadsafe(fut.set_exception, e)
                        
                except Exception as e:
                    logger.error(f"Error in trio bridge: {e}")
                    await trio.sleep(0.1)

    def run_trio_task(self, func: Callable[..., Awaitable[Any]], *args) -> asyncio.Future:
        """Called from asyncio to schedule a task in Trio."""
        if not self._running:
            raise RuntimeError("TrioBridge not running")
        
        fut = self._loop.create_future()
        self._request_queue.put((func, args, fut))
        return fut

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
