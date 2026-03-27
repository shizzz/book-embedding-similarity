const jobsContainer = document.getElementById("jobs");

async function fetchJobs() {
    const res = await fetch("/jobs/");
    const jobsData = await res.json();

    const jobsContainer = document.getElementById("jobs");

    const existingJobs = {};
    jobsContainer.querySelectorAll(".job").forEach(div => {
        const id = div.dataset.jobId;
        existingJobs[id] = div;
    });

    for (const [id, job] of Object.entries(jobsData)) {
        // Пропускаем пустой job
        const hasContent = (job.status && job.status.trim() !== "") ||
                           (job.message && job.message.trim() !== "") ||
                           (job.stats && Object.keys(job.stats.stages).length > 0);
        if (!hasContent) {
            // Если div уже есть, удаляем его
            if (existingJobs[id]) {
                existingJobs[id].remove();
            }
            continue;
        }

        let div;
        if (existingJobs[id]) {
            // уже есть — используем существующий
            div = existingJobs[id];
            div.innerHTML = `
                <div><b>${id}</b></div>
                <div>Status: ${job.status}</div>
                ${job.message ? `<div>Message: ${job.message}</div>` : ""}
                <pre id="stats-${id}"></pre>
            `;
        } else {
            // создаём новый div
            div = document.createElement("div");
            div.className = "job";
            div.dataset.jobId = id;
            div.innerHTML = `
                <div><b>${id}</b></div>
                <div>Status: ${job.status}</div>
                ${job.message ? `<div>Message: ${job.message}</div>` : ""}
                <pre id="stats-${id}"></pre>
            `;
            jobsContainer.appendChild(div);
        }

        // отрисуем таблицу по стадиям
        renderJobStats(id, job.stats);
    }

    for (const id of Object.keys(existingJobs)) {
        if (!jobsData[id]) {
            existingJobs[id].remove();
        }
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

async function runJob(entity, command, args) {
    const res = await fetch("/jobs/run", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            entity,
            command,
            args
        })
    });

    const data = await res.json();
    fetchJobs();
}

function renderJobStats(id, stats) {
    const statsContainer = document.getElementById(`stats-${id}`);
    if (!stats) {
        statsContainer.innerHTML = "-";
        return;
    }

    const table = document.createElement("table");
    table.className = "job-table";

    // заголовки
    table.innerHTML = `
        <tr>
            <th>Stage</th>
            <th>Progress</th>
            <th>Processed</th>
            <th>Total</th>
            <th>Queued</th>
            <th>Errors</th>
            <th>Speed</th>
            <th>ETA</th>
            <th>Status</th>
            <th>Pressure</th>
        </tr>
    `;

    const stages = stats.stages;
    for (const [stageName, st] of Object.entries(stages)) {
        const row = document.createElement("tr");

        const progressPct = st.total ? (st.processed / st.total) * 100 : 0;
        const progressBar = `<div class="bar-inner" style="width:${progressPct}%">${st.progress}</div>`;

        row.innerHTML = `
            <td>${stageName}${st.batch_size ? ` (${st.batch_size})` : ""}</td>
            <td class="progress-cell">${progressBar}</td>
            <td>${st.processed}</td>
            <td>${st.total ?? "-"}</td>
            <td>${st.queue}</td>
            <td>${st.errors}</td>
            <td>${st.speed}</td>
            <td>${st.eta}</td>
            <td>${st.finished ? "✓" : "RUN"}</td>
            <td>${(st.speed > 0 ? (st.queue / st.speed).toFixed(1) : 0)}</td>
        `;

        row.style.color = (st.queue / st.speed > 1 && !st.finished) ? "orange" : "inherit";

        table.appendChild(row);
    }

    statsContainer.innerHTML = "";
    statsContainer.appendChild(table);
}

// автообновление списка
setInterval(fetchJobs, 3000);

// первый запуск
fetchJobs();