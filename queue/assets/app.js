let activeTab = 'pending';
let latestJobs = [];
let registeredCrons = [];
let currentPage = 1;
let totalJobsCount = 0;
let searchQuery = '';
const selectedJobIDs = new Set();

let chart = null;
let chartLabels = [];
let throughputData = [];
let errorRateData = [];
let lastProcessed = null;
let lastFailed = null;
let lastTimestamp = null;

const chartConfig = {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [
            {
                label: 'Throughput (jobs/s)',
                data: throughputData,
                borderColor: 'rgba(99, 102, 241, 0.8)',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                borderWidth: 2,
                tension: 0.4,
                fill: true
            },
            {
                label: 'Errors (errors/s)',
                data: errorRateData,
                borderColor: 'rgba(239, 68, 68, 0.8)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                borderWidth: 2,
                tension: 0.4,
                fill: true
            }
        ]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            x: {
                grid: { color: 'rgba(255, 255, 255, 0.05)' },
                ticks: { color: '#94a3b8', font: { size: 10 } }
            },
            y: {
                grid: { color: 'rgba(255, 255, 255, 0.05)' },
                ticks: { color: '#94a3b8', font: { size: 10 } },
                beginAtZero: true
            }
        },
        plugins: {
            legend: {
                labels: { color: '#f1f5f9', font: { size: 11 } }
            }
        }
    }
};

function initChart() {
    const canvas = document.getElementById('performance-chart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    chart = new Chart(ctx, chartConfig);
}

function switchTab(tab) {
    activeTab = tab;
    currentPage = 1;
    selectedJobIDs.clear();
    const selectAllBox = document.getElementById('select-all-jobs');
    if (selectAllBox) selectAllBox.checked = false;
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-tab') === tab);
    });
    document.getElementById('dlq-bulk-actions').style.display = tab === 'dead' ? 'flex' : 'none';
    const bulkBar = document.getElementById('jobs-bulk-actions');
    if (bulkBar) bulkBar.style.display = 'none';
    renderTabContent();
}

function renderTabContent() {
    if (activeTab === 'cron') {
        renderCronJobs();
    } else {
        fetchJobs();
    }
}

function renderEmptyJobsState(tab) {
    const noJobs = document.getElementById('no-jobs');
    const table = document.getElementById('jobs-table');
    table.style.display = 'none';
    noJobs.style.display = 'block';
    noJobs.innerText = `No ${tab} jobs logs found.`;
    const pag = document.getElementById('jobs-pagination');
    if (pag) pag.style.display = 'none';
}

function createJobRow(j) {
    const tr = document.createElement('tr');
    tr.className = 'clickable-row';
    tr.addEventListener('click', (e) => {
        if (e.target.closest('button') || e.target.closest('input[type="checkbox"]')) return;
        openJobDetails(j.job_id);
    });
    const extraTd = activeTab === 'dead' ? `<td><div class="error-msg">${j.error_message || 'Unknown error'}</div></td>` : '';
    const actionsHtml = getJobActionsHtml(j);
    const isChecked = selectedJobIDs.has(j.job_id) ? 'checked' : '';
    tr.innerHTML = `
        <td style="text-align: center;"><input type="checkbox" class="job-select-checkbox" data-id="${j.job_id}" ${isChecked} onclick="toggleJobSelection(this, '${j.job_id}')"></td>
        <td style="font-family: monospace; font-size: 0.85rem; color: var(--text-muted);">${j.job_id}</td>
        <td style="font-weight: 600;">${j.queue}</td>
        <td>${j.name}</td>
        <td><span class="trace-id">${j.trace_id || 'N/A'}</span></td>
        ${extraTd}
        <td>${actionsHtml}</td>
    `;
    return tr;
}

function getJobActionsHtml(j) {
    if (activeTab === 'pending' || activeTab === 'running') {
        return `<button class="btn btn-danger" onclick="cancelJob('${j.job_id}')">Cancel</button>`;
    } else if (activeTab === 'dead') {
        return `
            <div class="actions-cell">
                <button class="btn btn-primary" onclick="retryJob('${j.job_id}')">Retry</button>
                <button class="btn btn-danger" onclick="cancelJob('${j.job_id}')">Cancel</button>
            </div>
        `;
    }
    return `<span style="color: var(--text-muted); font-size: 0.85rem;">None</span>`;
}

function renderJobs() {
    const tbody = document.getElementById('jobs-list');
    const noJobs = document.getElementById('no-jobs');
    const table = document.getElementById('jobs-table');
    const thExtra = document.getElementById('th-extra');

    document.getElementById('cron-table-wrapper').style.display = 'none';
    tbody.innerHTML = '';
    if (latestJobs.length === 0) {
        renderEmptyJobsState(activeTab);
        return;
    }
    table.style.display = 'table';
    noJobs.style.display = 'none';
    thExtra.style.display = activeTab === 'dead' ? 'table-cell' : 'none';
    latestJobs.forEach(j => tbody.appendChild(createJobRow(j)));
}

function createCronRow(c) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
        <td></td>
        <td style="font-weight: 600; color: #a5b4fc;">${c.name}</td>
        <td style="font-family: monospace; font-size: 0.85rem; color: var(--pending);">${c.expression}</td>
        <td style="font-weight: 600;">${c.queue}</td>
        <td><code style="font-family: monospace; font-size: 0.85rem; background: rgba(0,0,0,0.2); padding: 0.15rem 0.3rem; border-radius: 4px;">${c.payload || '{}'}</code></td>
    `;
    return tr;
}

function renderCronJobs() {
    const tbody = document.getElementById('cron-list');
    const noJobs = document.getElementById('no-jobs');
    const wrapper = document.getElementById('cron-table-wrapper');
    const table = document.getElementById('jobs-table');
    const pag = document.getElementById('jobs-pagination');
    
    table.style.display = 'none';
    if (pag) pag.style.display = 'none';
    tbody.innerHTML = '';
    if (registeredCrons.length === 0) {
        wrapper.style.display = 'none';
        noJobs.style.display = 'block';
        noJobs.innerText = 'No registered cron jobs found.';
        return;
    }
    wrapper.style.display = 'block';
    noJobs.style.display = 'none';
    registeredCrons.forEach(c => tbody.appendChild(createCronRow(c)));
}

async function retryJob(jobId) {
    if (!confirm(`Are you sure you want to retry job ${jobId}?`)) return;
    try {
        const res = await fetch(`/api/jobs/retry?id=${jobId}`, { method: 'POST' });
        if (!res.ok) throw new Error('Failed to retry job');
        fetchJobs();
    } catch (err) {
        alert(err.message);
    }
}

async function cancelJob(jobId) {
    if (!confirm(`Are you sure you want to cancel job ${jobId}?`)) return;
    try {
        const res = await fetch(`/api/jobs/cancel?id=${jobId}`, { method: 'POST' });
        if (!res.ok) throw new Error('Failed to cancel job');
        fetchJobs();
    } catch (err) {
        alert(err.message);
    }
}

async function clearQueue(queueName) {
    if (!confirm(`Are you sure you want to clear all jobs in queue "${queueName}"?`)) return;
    try {
        const res = await fetch(`/api/queues/clear?name=${queueName}`, { method: 'POST' });
        if (!res.ok) throw new Error('Failed to clear queue');
        fetchJobs();
    } catch (err) {
        alert(err.message);
    }
}

async function toggleQueue(queueName, action) {
    if (!confirm(`Are you sure you want to ${action} queue "${queueName}"?`)) return;
    try {
        const res = await fetch(`/api/queues/${action}?name=${queueName}`, { method: 'POST' });
        if (!res.ok) throw new Error(`Failed to ${action} queue`);
    } catch (err) {
        alert(err.message);
    }
}

async function fetchJobs() {
    if (activeTab === 'cron') return;
    try {
        const res = await fetch(`/api/jobs?q=${encodeURIComponent(searchQuery)}&status=${activeTab}&page=${currentPage}&limit=20`);
        if (!res.ok) throw new Error('Failed to fetch jobs');
        const data = await res.json();
        latestJobs = data.jobs || [];
        totalJobsCount = data.total || 0;
        renderJobs();
        updatePaginationControls();
    } catch (err) {
        console.error(err);
    }
}

function updatePaginationControls() {
    const wrapper = document.getElementById('jobs-pagination');
    if (!wrapper) return;
    if (latestJobs.length === 0 && currentPage === 1) {
        wrapper.style.display = 'none';
        return;
    }
    wrapper.style.display = 'flex';
    const maxPage = Math.max(1, Math.ceil(totalJobsCount / 20));
    document.getElementById('page-indicator').innerText = `Page ${currentPage} of ${maxPage}`;
    document.getElementById('btn-prev-page').disabled = currentPage <= 1;
    document.getElementById('btn-next-page').disabled = currentPage >= maxPage;
}

function prevPage() {
    if (currentPage > 1) {
        currentPage--;
        selectedJobIDs.clear();
        const selectAllBox = document.getElementById('select-all-jobs');
        if (selectAllBox) selectAllBox.checked = false;
        updateBulkButtons();
        fetchJobs();
    }
}

function nextPage() {
    const maxPage = Math.max(1, Math.ceil(totalJobsCount / 20));
    if (currentPage < maxPage) {
        currentPage++;
        selectedJobIDs.clear();
        const selectAllBox = document.getElementById('select-all-jobs');
        if (selectAllBox) selectAllBox.checked = false;
        updateBulkButtons();
        fetchJobs();
    }
}

function handleSearchInput() {
    searchQuery = document.getElementById('jobs-search').value;
    currentPage = 1;
    selectedJobIDs.clear();
    const selectAllBox = document.getElementById('select-all-jobs');
    if (selectAllBox) selectAllBox.checked = false;
    updateBulkButtons();
    fetchJobs();
}

function toggleSelectAllJobs(checkbox) {
    document.querySelectorAll('.job-select-checkbox').forEach(cb => {
        cb.checked = checkbox.checked;
        if (checkbox.checked) {
            selectedJobIDs.add(cb.dataset.id);
        } else {
            selectedJobIDs.delete(cb.dataset.id);
        }
    });
    updateBulkButtons();
}

function toggleJobSelection(checkbox, jobID) {
    if (checkbox.checked) {
        selectedJobIDs.add(jobID);
    } else {
        selectedJobIDs.delete(jobID);
    }
    const allChecked = Array.from(document.querySelectorAll('.job-select-checkbox')).every(cb => cb.checked);
    const selectAllBox = document.getElementById('select-all-jobs');
    if (selectAllBox) {
        selectAllBox.checked = allChecked && selectedJobIDs.size > 0;
    }
    updateBulkButtons();
}

function updateBulkButtons() {
    const container = document.getElementById('jobs-bulk-actions');
    if (container) {
        container.style.display = selectedJobIDs.size > 0 ? 'flex' : 'none';
    }
}

async function bulkAction(endpoint, confirmMsg) {
    if (selectedJobIDs.size === 0) return;
    if (!confirm(`${confirmMsg} ${selectedJobIDs.size} selected jobs?`)) return;
    try {
        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ids: Array.from(selectedJobIDs) })
        });
        if (!res.ok) throw new Error('Bulk operation failed');
        selectedJobIDs.clear();
        const selectAllBox = document.getElementById('select-all-jobs');
        if (selectAllBox) selectAllBox.checked = false;
        updateBulkButtons();
        fetchJobs();
    } catch (err) {
        alert(err.message);
    }
}

function bulkRetrySelected() {
    bulkAction('/api/jobs/bulk-retry', 'Are you sure you want to retry');
}

function bulkCancelSelected() {
    bulkAction('/api/jobs/bulk-cancel', 'Are you sure you want to cancel');
}

function bulkPurgeSelected() {
    bulkAction('/api/jobs/bulk-purge', 'Are you sure you want to purge');
}

function updateMainStats(data) {
    document.getElementById('val-pending').innerText = data.pending || 0;
    document.getElementById('val-running').innerText = data.running || 0;
    document.getElementById('val-processed').innerText = data.processed || 0;
    document.getElementById('val-failed').innerText = data.failed || 0;
}

function initChartData(now) {
    const timeStr = new Date(now).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    chartLabels.push(timeStr);
    throughputData.push(0);
    errorRateData.push(0);
    initChart();
}

function pushChartMetrics(now, totalProcessed, totalFailed) {
    const elapsedSec = (now - lastTimestamp) / 1000.0;
    if (elapsedSec <= 0) return;
    const pDiff = Math.max(0, totalProcessed - lastProcessed);
    const fDiff = Math.max(0, totalFailed - lastFailed);
    const throughput = Number((pDiff / elapsedSec).toFixed(2));
    const errorRate = Number((fDiff / elapsedSec).toFixed(2));
    const timeStr = new Date(now).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    
    chartLabels.push(timeStr);
    throughputData.push(throughput);
    errorRateData.push(errorRate);
    if (chartLabels.length > 15) {
        chartLabels.shift();
        throughputData.shift();
        errorRateData.shift();
    }
    if (!chart) initChart();
    else chart.update();
}

function updatePerformanceChart(data) {
    const now = Date.now();
    const totalProcessed = data.processed || 0;
    const totalFailed = data.failed || 0;
    if (lastProcessed !== null && lastTimestamp !== null) {
        pushChartMetrics(now, totalProcessed, totalFailed);
    } else {
        initChartData(now);
    }
    lastProcessed = totalProcessed;
    lastFailed = totalFailed;
    lastTimestamp = now;
}

function createQueueRow(q) {
    const tr = document.createElement('tr');
    const pb = q.paused ? '<span class="badge" style="background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.2); color: #fbbf24; margin-left: 0.5rem; font-size: 0.7rem; padding: 0.15rem 0.4rem; vertical-align: middle;">Paused</span>' : '';
    const btn = q.paused ? 'resume' : 'pause';
    const label = q.paused ? 'Resume' : 'Pause';
    const btnClass = q.paused ? 'btn-primary' : 'btn-warning';
    const act = `<button class="btn ${btnClass}" onclick="toggleQueue('${q.name}', '${btn}')">${label}</button>`;
    tr.innerHTML = `
        <td style="font-weight: 600;">${q.name}${pb}</td>
        <td style="color: var(--pending);">${q.pending}</td>
        <td style="color: var(--running);">${q.running}</td>
        <td style="color: var(--running);">${q.processed || 0}</td>
        <td style="color: var(--failed);">${q.failed}</td>
        <td>
            <div class="actions-cell">
                ${act}
                <button class="btn btn-danger" onclick="clearQueue('${q.name}')">Clear</button>
            </div>
        </td>
    `;
    return tr;
}

function renderQueues(queues) {
    const tbody = document.getElementById('queues-list');
    const noQueues = document.getElementById('no-queues');
    const table = document.getElementById('queues-table');
    
    tbody.innerHTML = '';
    if (queues.length === 0) {
        table.style.display = 'none';
        noQueues.style.display = 'block';
        return;
    }
    table.style.display = 'table';
    noQueues.style.display = 'none';
    queues.forEach(q => tbody.appendChild(createQueueRow(q)));
}

function createProcessRow(p) {
    const tr = document.createElement('tr');
    const hTime = new Date(p.heartbeat_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const queuesStr = p.queues ? p.queues.join(', ') : 'None';
    tr.innerHTML = `
        <td style="font-family: monospace; font-size: 0.85rem; color: var(--text-muted);">${p.process_id}</td>
        <td style="font-weight: 600;">${p.concurrency}</td>
        <td>${queuesStr}</td>
        <td style="color: var(--running);">${hTime}</td>
    `;
    return tr;
}

function renderProcesses(processes) {
    const tbody = document.getElementById('processes-list');
    const noProcesses = document.getElementById('no-processes');
    const table = document.getElementById('processes-table');
    
    tbody.innerHTML = '';
    if (processes.length === 0) {
        table.style.display = 'none';
        noProcesses.style.display = 'block';
        return;
    }
    table.style.display = 'table';
    noProcesses.style.display = 'none';
    processes.forEach(p => tbody.appendChild(createProcessRow(p)));
}

async function openJobDetails(jobId) {
    try {
        const res = await fetch(`/api/jobs/detail?id=${jobId}`);
        if (!res.ok) throw new Error('Failed to fetch job details');
        const data = await res.json();
        populateModal(data);
        document.getElementById('job-details-modal').style.display = 'flex';
    } catch (err) {
        alert(err.message);
    }
}

function populateModal(data) {
    document.getElementById('modal-job-id').innerText = data.job_id || '';
    document.getElementById('modal-queue').innerText = data.queue || '';
    document.getElementById('modal-name').innerText = data.name || '';
    document.getElementById('modal-attempts').innerText = data.attempts || 0;
    document.getElementById('modal-max-attempts').innerText = data.max_attempts || 0;
    document.getElementById('modal-run-at').innerText = data.run_at || 'N/A';
    document.getElementById('modal-trace-id').innerText = data.trace_context?.trace_id || 'N/A';
    document.getElementById('modal-span-id').innerText = data.trace_context?.span_id || 'N/A';
    document.getElementById('modal-unique-key').innerText = data.unique_key || 'N/A';
    document.getElementById('modal-batch-id').innerText = data.batch_id || 'N/A';
    renderPayload(data.args);
}

function renderPayload(args) {
    let formatted = '{}';
    try {
        const decoded = atob(args);
        formatted = JSON.stringify(JSON.parse(decoded), null, 2);
    } catch (err) {
        formatted = args ? atob(args) : '{}';
    }
    document.getElementById('modal-payload').innerText = formatted;
}

function closeJobDetailsModal() {
    document.getElementById('job-details-modal').style.display = 'none';
}

function copyPayloadToClipboard() {
    const payload = document.getElementById('modal-payload').innerText;
    navigator.clipboard.writeText(payload).then(() => {
        alert('Payload copied to clipboard!');
    });
}

async function retryAllFailed() {
    if (!confirm('Are you sure you want to retry all failed/dead jobs?')) return;
    try {
        const res = await fetch('/api/jobs/failed/retry', { method: 'POST' });
        if (!res.ok) throw new Error('Failed to retry all jobs');
    } catch (err) {
        alert(err.message);
    }
}

async function purgeAllFailed() {
    if (!confirm('Are you sure you want to purge all failed/dead jobs permanently?')) return;
    try {
        const res = await fetch('/api/jobs/failed/purge', { method: 'POST' });
        if (!res.ok) throw new Error('Failed to purge all jobs');
    } catch (err) {
        alert(err.message);
    }
}

// Connect to SSE metrics stream
function connectStatsStream() {
    const stream = new EventSource('/api/stats/stream');
    stream.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            updateMainStats(data);
            updatePerformanceChart(data);
            renderQueues(data.queues || []);
            renderProcesses(data.processes || []);
            registeredCrons = data.cron_jobs || [];
            if (activeTab === 'cron') {
                renderCronJobs();
            }
        } catch (err) {
            console.error("SSE parse error", err);
        }
    };
    stream.onerror = () => {
        stream.close();
        setTimeout(connectStatsStream, 3000);
    };
}

// Initial pull and setup
fetchJobs();
connectStatsStream();
setInterval(fetchJobs, 5000);
