es.onmessage = function(e) {
    try {
        const data = JSON.parse(e.data);

        if (data.type === "progress") {
            document.getElementById("status").innerText = "Идёт поиск похожих книг…";
            document.getElementById("status").className = "loading";
            document.getElementById("progress").innerText = `Прогресс: ${data.progress}%`;
        }

        else if (data.type === "done") {
            es.close();
            document.getElementById("status").innerText = "Готово!";
            document.getElementById("status").className = "";
            document.getElementById("progress").innerText = "";
            document.getElementById("result").innerHTML = data.html;
        }

        else if (data.type === "error") {
            es.close();
            document.getElementById("status").innerText = "Ошибка";
            document.getElementById("status").className = "error";
            document.getElementById("progress").innerText = "";
            document.getElementById("result").innerHTML = 
                `<p class="error">Не удалось обработать запрос: ${data.message || 'неизвестная ошибка'}</p>`;
        }
    } catch (err) {
        console.error("Ошибка парсинга SSE:", err);
        document.getElementById("status").innerText = "Ошибка соединения";
        document.getElementById("status").className = "error";
    }
};

es.onerror = function() {
    console.error("Ошибка EventSource");
    es.close();
    document.getElementById("status").innerText = "Потеряно соединение с сервером";
    document.getElementById("status").className = "error";
    document.getElementById("progress").innerText = "";
};

document.addEventListener('click', async function(e) {
    if (e.target.classList.contains('like-btn') || e.target.classList.contains('dislike-btn')) {
        e.preventDefault();
        const btn = e.target;
        const row = btn.closest('tr');
        if (!row) return;

        const candidate = row.dataset.candidate;
        const action = btn.dataset.action;

        try {
            const resp = await fetch('/feedback', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source_file_name: '{{ file | e }}',  // из контекста страницы
                    candidate_file_name: candidate,
                    label: parseInt(action)
                })
            });

            if (resp.ok) {
                row.querySelectorAll('button').forEach(b => {
                    b.style.opacity = '0.4';
                    b.disabled = true;
                });
                btn.style.opacity = '1';
                btn.style.fontWeight = 'bold';
            } else {
                alert('Ошибка отправки');
            }
        } catch (err) {
            alert('Ошибка сети: ' + err.message);
        }
    }
});