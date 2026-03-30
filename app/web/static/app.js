const jobsContainer = document.getElementById("jobs");

let COMMAND_GROUPS = {};

async function loadCommandGroups() {
    const res = await fetch("/commands");
    COMMAND_GROUPS = await res.json();
}

// ------------------- Render Command Groups -------------------
function renderCommandGroups() {
    const container = document.getElementById("command-groups");
    container.innerHTML = '';

    for (const [groupName, commands] of Object.entries(COMMAND_GROUPS)) {
        const groupDiv = document.createElement("div");
        groupDiv.classList.add("command-group");
        groupDiv.classList.add("collapsed");

        // заголовок группы с раскрытием
        const h3 = document.createElement("h3");
        h3.textContent = groupName + " ▼";
        h3.style.cursor = "pointer";
        h3.onclick = () => {
            groupDiv.classList.toggle("collapsed");
            h3.textContent = groupDiv.classList.contains("collapsed")
                ? groupName + " ▶"
                : groupName + " ▼";
        };
        groupDiv.appendChild(h3);

        const commandsDiv = document.createElement("div");
        commandsDiv.classList.add("commands");

        commands.forEach(cmd => {
            const cmdDiv = document.createElement("div");
            cmdDiv.classList.add("command");

            // input’ы для аргументов
            for (const [key, meta] of Object.entries(cmd.args || {})) {
                const label = document.createElement("label");
                label.textContent = key;

                let input;

                // --- select (choices) ---
                if (meta.choices) {
                    input = document.createElement("select");

                    meta.choices.forEach(opt => {
                        const option = document.createElement("option");
                        option.value = opt;
                        option.textContent = opt;
                        input.appendChild(option);
                    });

                    if (meta.default) input.value = meta.default;
                }

                // --- boolean ---
                else if (meta.type === "bool") {
                    input = document.createElement("input");
                    input.type = "checkbox";
                    input.checked = meta.default;
                }

                // --- number ---
                else if (meta.type === "int" || meta.type === "float") {
                    input = document.createElement("input");
                    input.type = "number";
                    input.value = meta.default ?? 0;
                }

                // --- list ---
                else if (meta.type === "list") {
                    input = document.createElement("input");
                    input.type = "text";
                    input.value = (meta.default || []).join(",");
                    input.placeholder = "a,b,c";
                }

                // --- string ---
                else {
                    input = document.createElement("input");
                    input.type = "text";
                    input.value = meta.default ?? "";
                }

                if (meta.required) {
                    input.required = true;
                }

                input.name = key;

                label.appendChild(input);
                cmdDiv.appendChild(label);
            }

            // кнопка запуска
            const btn = document.createElement("button");
            btn.textContent = cmd.label;
            btn.onclick = () => {
                const args = {};
                cmdDiv.querySelectorAll("input, select").forEach(input => {
                    if (input.type === "checkbox") {
                        args[input.name] = input.checked;
                    } else if (input.type === "number") {
                        args[input.name] = Number(input.value);
                    } else if (input.tagName === "SELECT") {
                        args[input.name] = input.value;
                    } else {
                        // list support
                        if (input.value.includes(",")) {
                            args[input.name] = input.value.split(",").map(v => v.trim());
                        } else {
                            args[input.name] = input.value;
                        }
                    }
                });
                runJob(cmd.entity, cmd.command, args);
            };
            btn.dataset.entity = cmd.entity;
            btn.dataset.command = cmd.command;

            cmdDiv.appendChild(btn);

            commandsDiv.appendChild(cmdDiv);
        });

        groupDiv.appendChild(commandsDiv);
        container.appendChild(groupDiv);
    }
}

// ------------------- Jobs -------------------
async function fetchJobs() {
    const res = await fetch("/jobs/");
    const jobsData = await res.json();

    const existingJobs = {};
    jobsContainer.querySelectorAll(".job").forEach(div => existingJobs[div.dataset.jobId] = div);

    for (const [id, job] of Object.entries(jobsData)) {
        // пропускаем пустые job
        const hasContent = (job.status && job.status.trim() !== "") ||
                           (job.message && job.message.trim() !== "") ||
                           (job.stats && Object.keys(job.stats?.stages ?? {}).length > 0);
        if (!hasContent) {
            existingJobs[id]?.remove();
            continue;
        }

        let div;
        if (existingJobs[id]) {
            div = existingJobs[id];
            div.innerHTML = renderJobHTML(id, job);
        } else {
            div = document.createElement("div");
            div.className = "job";
            div.dataset.jobId = id;
            div.innerHTML = renderJobHTML(id, job);
            jobsContainer.appendChild(div);
        }

        renderJobStats(id, job.stats);
    }

    for (const id of Object.keys(existingJobs)) {
        if (!jobsData[id]) existingJobs[id].remove();
    }
}

function renderJobHTML(id, job) {
    return `
        <div><b>${id}</b></div>
        <div>Status: ${job.status}</div>
        ${job.message ? `<div>Message: ${job.message}</div>` : ""}
        <div class="progress"><div class="bar" id="bar-${id}"></div></div>
        <pre id="stats-${id}"></pre>
    `;
}

function subscribe(jobId) {
    const ws = new WebSocket(`ws://${location.host}/ws/${jobId}`);
    ws.onmessage = event => {
        const data = JSON.parse(event.data);
        let progress = 0;

        if (data.stats?.stages) {
            const stages = Object.values(data.stats.stages);
            if (stages.length > 0) progress = stages[0].progress ? stages[0].progress * 100 : 0;
        }

        const bar = document.getElementById(`bar-${jobId}`);
        if (bar) bar.style.width = progress + "%";

        const stats = document.getElementById(`stats-${jobId}`);
        if (stats) stats.textContent = JSON.stringify(data.stats, null, 2);
    };
}

async function runJob(entity, command, args) {
    const btn = document.querySelector(`.command button[data-entity="${entity}"][data-command="${command}"]`);
    if (btn) {
        btn.disabled = true;
        const origText = btn.textContent;
        btn.textContent = "Running…";
    }

    try {
        const res = await fetch("/jobs/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ entity, command, args })
        });
        const data = await res.json();
        console.log(data);
        fetchJobs();
    } catch (err) {
        console.error(err);
        alert("Ошибка при запуске команды!");
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = origText;
        }
    }
}

function renderJobStats(id, stats) {
    const statsContainer = document.getElementById(`stats-${id}`);
    if (!stats) {
        statsContainer.textContent = "-";
        return;
    }

    const table = document.createElement("table");
    table.className = "job-table";
    table.innerHTML = `
        <tr>
            <th>Stage</th><th>Progress</th><th>Processed</th><th>Total</th>
            <th>Queued</th><th>Errors</th><th>Speed</th><th>ETA</th>
            <th>Status</th><th>Pressure</th>
        </tr>
    `;

    const stages = stats.stages ?? {};
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
        row.style.color = (st.speed && st.queue / st.speed > 1 && !st.finished) ? "orange" : "inherit";
        table.appendChild(row);
    }

    statsContainer.innerHTML = "";
    statsContainer.appendChild(table);
}

// запуск
async function init() {
    await loadCommandGroups();
    renderCommandGroups();
    fetchJobs();
}

// автообновление
setInterval(fetchJobs, 3000);

init();