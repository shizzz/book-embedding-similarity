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
    TaskID
)
from .ui import BaseUI
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
        self._max_workers = max_workers
        self._show_table = show_table
        
        self._label = title
        self._bars: Dict[int, Progress] = {}
        self._tasks: Dict[int, TaskID] = {}
        self._speed_history = {}

        self.lock = asyncio.Lock()
        self.console = Console()

    async def update(self):
        async with self.lock:
            self.live.update(self.layout())

    def init(self):
        self.live = Live(self.layout(), refresh_per_second=1, console=self.console)
        self.live.start()
        
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
        stage_items = self.pipeline_stats.get_ordered_stages(mode="topology")

        # -------------------------
        # определяем bottleneck
        # -------------------------
        bottleneck_stage = None

        # проверяем starvation (все очереди ниже пустые)
        downstream_empty = True
        for _, st in stage_items[1:]:
            if st.queue > 0 and not st.finished:
                downstream_empty = False
                break

        if downstream_empty:
            # bottleneck первый незавершённый stage
            for stage_name, st in stage_items:
                if not st.finished:
                    bottleneck_stage = stage_name
                    break
        else:
            max_pressure = -1

            for stage_name, st in stage_items:
                if st.finished:
                    continue

                if st.speed_value <= 0:
                    pressure = float("inf")
                else:
                    pressure = st.queue / st.speed_value

                if pressure > max_pressure:
                    max_pressure = pressure
                    bottleneck_stage = stage_name

        # -------------------------
        # строим таблицу
        # -------------------------
        for stage_name, st in stage_items:
            if st.total:
                pct = st.processed / st.total
                filled = int(pct * 20)
                progress_bar = "█" * filled + "░" * (20 - filled)
                progress = f"{progress_bar} {st.percent}"
            else:
                progress = "-"

            stage_str = f"{stage_name} ({st.batch_size})" if st.batch_size else stage_name
            stage_text = Text(stage_str)

            status = "✓" if st.finished else "RUN"

            if st.speed_value <= 0:
                pressure_value = 0
            else:
                pressure_value = st.queue / st.speed_value

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

    def _make_model_info(self):
        left = Table.grid(padding=(0, 1))
        left.add_column(style="cyan")
        left.add_column()

        left.add_row("Model", self.pipeline_stats.model_info.model_name or "-")
        left.add_row("UID", self.pipeline_stats.model_info.uid or "-")

        overlap_pct = int(self.pipeline_stats.model_info.st_overlap / self.pipeline_stats.model_info.max_seq_length * 100)

        left.add_row("Chunk size", f"{self.pipeline_stats.model_info.max_seq_length}")
        left.add_row("Overlap", f"{self.pipeline_stats.model_info.st_overlap} ({overlap_pct}%)")

        batch_style = "green"
        left.add_row(
            "Batch tokens",
            Text(str(self.pipeline_stats.model_info.tokens_per_batch), style=batch_style)
        )

        left.add_row(
            "Mem/token est",
            self._fmt_mb(self.pipeline_stats.model_info.estimate_mem_per_token_mb)
        )

        left.add_row(
            "Inc/Dec",
            Text.assemble(
                (str(self.pipeline_stats.model_info.increases), "green"),
                ("/",),
                (str(self.pipeline_stats.model_info.decreases), "red"),
            )
        )

        # GPU часть
        right = Table.grid(padding=(0, 1))
        right.add_column(style="cyan")
        right.add_column()

        cuda_text = Text("YES", style="green") if self.pipeline_stats.model_info.cuda_available else Text("NO", style="red")
        right.add_row("CUDA", cuda_text)

        if self.pipeline_stats.model_info.cuda_available:

            right.add_row("CUDA ver", self.pipeline_stats.model_info.cuda_version)
            right.add_row("GPU", self.pipeline_stats.model_info.gpu_name)
            right.add_row("GPU count", str(self.pipeline_stats.model_info.gpu_count))

            free = self.pipeline_stats.model_info.free_vram_mb
            total = self.pipeline_stats.model_info.total_vram_mb

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
                if self.pipeline_stats.model_info.temp > 75:
                    temp_style = "yellow"
                if self.pipeline_stats.model_info.temp > 85:
                    temp_style = "red"

                right.add_row(
                    "GPU temp",
                    Text(f"{self.pipeline_stats.model_info.temp}°C", style=temp_style)
                )

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column()

        grid.add_row(left, right)

        return Panel(grid, title="Embedding Model", border_style="blue")

    def layout(self) -> Table:
        grid = Table.grid(expand=True)
        try:
            if self._show_table:
                grid.add_row(self._make_stats_table())
                grid.add_row(self._make_edges_table())
                if self.pipeline_stats.model_info:
                    grid.add_row(self._make_model_info())
                #grid.add_row(self._make_info())

            for idx in self._bars:
                grid.add_row(self._bars[idx])
        except Exception as e:
            print(e)
        return grid

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