import asyncio
from typing import Dict
from rich.live import Live
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress_bar import ProgressBar
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TaskProgressColumn,
    TaskID
)
from time import perf_counter
from .ui import BaseUI
from .tqdmLike import TqdmLike, TqdmIterable
from app.workers.stats import Stats
from app.infrastructure.models import Report

class LiveUI(BaseUI):
    def __init__(
            self,
            max_workers: int,
            stats: Stats,
            title: str = "Library scanner",
            show_table: bool = True
        ):
        self.live: Live = None
        self.pipeline_stats: Stats = stats
        self.model_info = None
        self._max_workers = max_workers
        self._show_table = show_table
        
        self._label = title
        self._bars: Dict[int, Progress] = {}
        self._tasks: Dict[int, TaskID] = {}
        self._speed_history = {} 

        self.stats = {
            "Total": 0,
            "Remaining": 0,
            "Done": 0,
            "Errors": 0,
        }
        for i in range(1, max_workers + 1):
            self.stats[f"Thread {i}"] = "-"

        self.lock = asyncio.Lock()
        self.console = Console()

    async def update(self):
        async with self.lock:
            self.live.update(self.layout())

    def _make_stats_table(self) -> Table:
        table = Table(expand=True)

        table.add_column("Stage")
        table.add_column("Progress")
        table.add_column("Processed")
        table.add_column("Total")
        table.add_column("Queued")
        table.add_column("Errors")
        table.add_column("Speed")
        table.add_column("ETA")
        table.add_column("Status")
        table.add_column("Pressure")

        stages = self.pipeline_stats.stages

        # определяем bottleneck
        bottleneck_stage = None
        max_score = 0

        for stage_name, st in stages.items():
            if st.finished:
                continue
            queue_ratio = st.queue / st.queue_max_size if st.queue_max_size else 0
            pressure = st.queue / max(st.speed_value, 1)
            score = pressure * (1 + queue_ratio)

            if not st.finished and score > max_score:
                max_score = score
                bottleneck_stage = stage_name

        for stage_name, st in stages.items():
            if st.total:
                pct = st.processed / st.total
                filled = int(pct * 20)
                progress_bar = "█" * filled + "░" * (20 - filled)
                progress = f"{progress_bar} {st.percent}"
            else:
                progress = "-"

            stage_text = Text(stage_name)

            status = "✓" if st.finished else "RUN"

            pressure_value = st.queue / max(st.speed_value, 1)
            pressure_str = f"{pressure_value:.1f}"
            pressure_text = Text(pressure_str)

            if pressure_value > 1 and not st.finished:
                stage_text.stylize("yellow")
                pressure_text.stylize("yellow")

            if stage_name == bottleneck_stage and not st.finished:
                stage_text.stylize("bold red")
                pressure_text.stylize("bold red")

            table.add_row(
                stage_text,
                progress,
                str(st.processed),
                str(st.total) if st.total else "-",
                str(st.queue),
                str(st.errors),
                st.speed,
                st.eta,
                status,
                pressure_text
            )

        table.caption = f"Runtime: {self.pipeline_stats.runtime}"
        return table
    
    def _make_edges_table(self):
        table = Table(title="Edges", expand=True)

        table.add_column("From")
        table.add_column("To")
        table.add_column("Count")
        table.add_column("Speed")

        for edge in self.pipeline_stats.edges.values():
            table.add_row(
                edge.upstream,
                edge.downstream,
                str(edge.count),
                edge.speed
            )

        return table

    def _make_info(self) -> Text:
        info = Text()
        for i in range(1, self._max_workers + 1):
            key = f"Thread {i}"
            info.append(f"{key}: {self.stats[key]}\n")
        return info

    def _compute_speed(self, idx: int, count: int) -> str:
        now = perf_counter()

        if idx not in self._speed_history:
            self._speed_history[idx] = {
                "last_time": now,
                "ema_speed": 0.0
            }
            return "0"

        data = self._speed_history[idx]

        dt = now - data["last_time"]
        data["last_time"] = now

        if dt <= 0:
            return "0"

        instant_speed = count / dt

        alpha = 0.2  # чем меньше — тем плавнее

        if data["ema_speed"] == 0:
            data["ema_speed"] = instant_speed
        else:
            data["ema_speed"] = (
                alpha * instant_speed +
                (1 - alpha) * data["ema_speed"]
            )

        speed = data["ema_speed"]

        if speed >= 10:
            speed_fmt = f"{speed:.0f}"
        elif speed >= 1:
            speed_fmt = f"{speed:.1f}"
        else:
            speed_fmt = f"{speed:.2f}"

        return speed_fmt
        
    def layout(self) -> Table:
        grid = Table.grid(expand=True)
        try:
            if self._show_table:
                grid.add_row(self._make_stats_table())
                grid.add_row(self._make_edges_table())
                if self.model_info:
                    grid.add_row(self._make_model_info())
                #grid.add_row(self._make_info())

            for idx in self._bars:
                grid.add_row(self._bars[idx])
        except Exception as e:
            print(e)
        return grid
    
    def init(self):
        self.live = Live(self.layout(), refresh_per_second=1, console=self.console)
        self.live.start()

    async def set_thread(self, worker_id: int, name: str):
        async with self.lock:
            self.stats[f"Thread {worker_id}"] = name
        self.live.update(self.layout())

    async def done_async(self, idx: int = 0, count: int = 1):
        speed = self._compute_speed(idx, count)
        async with self.lock:
            if idx == 0:
                self.stats["Done"] += count
                self.stats["Remaining"] -= count

            self._bars[idx].update(
                task_id=self._tasks[idx],
                advance=count,
                custom_speed=speed
            )
            self.live.update(self.layout())

    def done(self, idx: int = 0, count: int = 1):
        speed = self._compute_speed(idx, count)
        self._bars[idx].update(
            task_id=self._tasks[idx], 
            advance=count,
            custom_speed=speed
        )
        self.live.update(self.layout())

    async def update_total_async(self, total: int, idx: int = 0):
        async with self.lock:
            if idx == 0:
                old_total = self.stats["Total"]
                done = self.stats["Done"]

                if old_total < 0:
                    total = total + old_total

                self.stats["Total"] = total
                self.stats["Remaining"] = max(total - done, 0)

            self._bars[idx].update(self._tasks[idx], total=total)

    def update_total(self, total: int, idx: int = 0):
        self._bars[idx].update(self._tasks[idx], total=total)

    async def decrease_total_async(self, decrease: int = 1):
        async with self.lock:
            old_total = self.stats["Total"]
            total = old_total - decrease

            self.stats["Total"] = total
            self.stats["Remaining"] -= decrease

            self._bars[0].update(self._tasks[0], total=total)

    async def error(self, idx: int = 0):
        async with self.lock:
            self.stats["Errors"] += 1

        self._bars[idx].update(self._tasks[idx], advance=1)
        self.live.update(self.layout())

    def add_progress(
            self,
            descr: str, 
            unit: str,
            show_elapsed: bool = False
        ) -> int:
        idx = max(self._bars.keys(), default=-1) + 1

        fixed_len = 25
        descr_fixed = descr.ljust(fixed_len)[:fixed_len]

        columns = [
            TextColumn("[bold cyan]{task.description:>20}", justify="left"),
            BarColumn(bar_width=40),
            TextColumn("{task.completed:>6}/{task.total:<6}", justify="right"),
            TaskProgressColumn(),
            TextColumn(
                "{task.fields[custom_speed]} {task.fields[unit]}/с",
                justify="right",
            ),
            TimeRemainingColumn(),
        ]
        if show_elapsed:
            columns.append(TimeElapsedColumn())

        self._bars[idx] = Progress(*columns)

        self._tasks[idx] = self._bars[idx].add_task(
            description=f"[bold green]{descr_fixed}",
            total=0,
            unit=unit,
            custom_speed=0
        )

        return idx
    
    def remove_progress(self, idx: int) -> None:
        if idx not in self._bars:
            return

        bar = self._bars[idx]

        try:
            bar.stop()
        except Exception:
            pass

        del self._bars[idx]

        if idx in self._tasks:
            del self._tasks[idx]
        self.live.update(self.layout())

    def _make_model_info(self):
        left = Table.grid(padding=(0, 1))
        left.add_column(style="cyan")
        left.add_column()

        left.add_row("Model", self.model_info.model_name or "-")
        left.add_row("UID", self.model_info.uid or "-")

        overlap_pct = int(self.model_info.st_overlap / self.model_info.st_chunk_size * 100)

        left.add_row("Chunk size", f"{self.model_info.st_chunk_size}")
        left.add_row("Overlap", f"{self.model_info.st_overlap} ({overlap_pct}%)")

        batch_style = "green"
        if self.model_info.st_batch_size > 128:
            batch_style = "yellow"
        if self.model_info.st_batch_size > 256:
            batch_style = "red"

        left.add_row(
            "Batch size",
            Text(str(self.model_info.st_batch_size), style=batch_style)
        )

        left.add_row(
            "Mem/chunk est",
            self._fmt_mb(self.model_info.estimate_mem_per_chunk_mb)
        )

        if self.model_info.measure_mem_per_chunk_mb:
            left.add_row(
                "Mem/chunk real",
                self._fmt_mb(self.model_info.measure_mem_per_chunk_mb)
            )

        # GPU часть
        right = Table.grid(padding=(0, 1))
        right.add_column(style="cyan")
        right.add_column()

        cuda_text = Text("YES", style="green") if self.model_info.cuda_available else Text("NO", style="red")
        right.add_row("CUDA", cuda_text)

        if self.model_info.cuda_available:

            right.add_row("CUDA ver", self.model_info.cuda_version)
            right.add_row("GPU", self.model_info.gpu_name)
            right.add_row("GPU count", str(self.model_info.gpu_count))

            free = self.model_info.free_vram_mb
            total = self.model_info.total_vram_mb

            if free and total:
                used = total - free

                bar = ProgressBar(
                    total=total,
                    completed=used,
                    width=28
                )

                right.add_row(
                    "VRAM",
                    f"{self._fmt_mb(used)} / {self._fmt_mb(total)}"
                )
                right.add_row("", bar)

                temp_style = "green"
                if self.model_info.temp > 75:
                    temp_style = "yellow"
                if self.model_info.temp > 85:
                    temp_style = "red"

                right.add_row(
                    "GPU temp",
                    Text(f"{self.model_info.temp}°C", style=temp_style)
                )

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column()

        grid.add_row(left, right)

        return Panel(grid, title="Embedding Model", border_style="blue")

    def tqdm(self, obj=None, desc: str = "", total: int = 0, unit: str = "", show_elapsed: bool = False):
        if hasattr(obj, "__iter__") and obj is not None:
            return TqdmIterable(self, obj, desc, unit, show_elapsed)
        return TqdmLike(self, desc, total, unit, show_elapsed)
    
    def report(self, report: Report):
        """Красивый компактный отчет всех блоков"""
        grid = Table.grid(expand=True)
        
        for block in report.blocks:
            # создаём небольшую таблицу для блока
            block_table = Table(show_header=False, box=None, expand=True)
            block_table.add_column("Metric", style="bold cyan", no_wrap=True)
            block_table.add_column("Value", justify="right", no_wrap=False)

            for metric in block.metrics:
                if metric.extra and isinstance(metric.extra, list):
                    value = "\n".join(str(x) for x in metric.extra)
                else:
                    value = metric.value if metric.value is not None else "-"
                block_table.add_row(metric.name, str(value))
            
            # добавляем блок в общий grid
            grid.add_row(Panel(block_table, title=block.title, expand=True))
        
        self.console.print(grid)

    def _fmt_mb(self, mb: int) -> str:
        if mb > 1024:
            return f"{mb/1024:.1f} GB"
        return f"{mb} MB"