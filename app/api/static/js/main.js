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
        
        document.querySelectorAll(".star-rating").forEach(container => {
            const current = parseFloat(container.dataset.current || 0);
            renderStars(container, current);
        });
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
    if (e.target.classList.contains('star')) {
        e.preventDefault();

        const star = e.target;
        const row = star.closest('tr');
        if (!row) return;

        const container = star.closest('.star-rating');
        const source = row.dataset.source;
        const candidate = row.dataset.candidate;

        const stars = parseInt(star.dataset.value);
        const rating = stars * 0.2;

        await sendRating(row, container, source, candidate, rating);
    }

    if (e.target.classList.contains('reset-rating')) {
        e.preventDefault();

        const reset = e.target;
        const row = reset.closest('tr');
        if (!row) return;

        const container = reset.closest('.star-rating');
        const source = row.dataset.source;
        const candidate = row.dataset.candidate;

        await sendRating(row, container, source, candidate, 0);
    }

    if (e.target.classList.contains('negative-rating')) {
        e.preventDefault();

        const reset = e.target;
        const row = reset.closest('tr');
        if (!row) return;

        const container = reset.closest('.star-rating');
        const source = row.dataset.source;
        const candidate = row.dataset.candidate;

        await sendRating(row, container, source, candidate, -1);
        row.remove();
    }
});

async function sendRating(row, container, source, candidate, rating) {
    try {
        const resp = await fetch('/similar/feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                source_file_name: source,
                candidate_file_name: candidate,
                label: rating
            })
        });

        if (!resp.ok) {
            alert('Ошибка отправки');
            return;
        }

        container.dataset.current = rating;
        renderStars(container, rating);

    } catch (err) {
        alert('Ошибка сети: ' + err.message);
    }
}

function renderStars(container, ratingFloat) {
    const stars = container.querySelectorAll(".star");
    const activeStars = Math.round(ratingFloat / 0.2);

    stars.forEach(star => {
        const value = parseInt(star.dataset.value);
        if (value <= activeStars) {
            star.classList.add("active");
        } else {
            star.classList.remove("active");
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll(".star-rating").forEach(container => {
        const current = parseFloat(container.dataset.current || 0);
        renderStars(container, current);
    });
});