const jobsContainer = document.getElementById("jobs");

async function fetchJobs() {
    const res = await fetch("/jobs/");
    const jobs = await res.json();

    jobsContainer.innerHTML = "";

    for (const [id, job] of Object.entries(jobs)) {
        const div = document.createElement("div");
        div.className = "job";

        div.innerHTML = `
            <div><b>${id}</b></div>
            <div>Status: ${job.status}</div>
            <div class="progress">
                <div class="bar" id="bar-${id}"></div>
            </div>
            <pre id="stats-${id}"></pre>
        `;

        jobsContainer.appendChild(div);

        subscribe(id);
    }
}

function subscribe(jobId) {
    const ws = new WebSocket(`ws://${location.host}/ws/${jobId}`);

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        // прогресс
        let progress = 0;

        if (data.stats?.stages) {
            const stages = Object.values(data.stats.stages);

            if (stages.length > 0) {
                const s = stages[0];
                progress = s.progress ? s.progress * 100 : 0;
            }
        }

        const bar = document.getElementById(`bar-${jobId}`);
        if (bar) {
            bar.style.width = progress + "%";
        }

        const stats = document.getElementById(`stats-${jobId}`);
        if (stats) {
            stats.textContent = JSON.stringify(data.stats, null, 2);
        }
    };
}

async function runJob(entity, command) {
    const res = await fetch(`/jobs/run?entity=${entity}&command=${command}`, {
        method: "POST"
    });

    const data = await res.json();

    fetchJobs();
}

// автообновление списка
setInterval(fetchJobs, 3000);

// первый запуск
fetchJobs();