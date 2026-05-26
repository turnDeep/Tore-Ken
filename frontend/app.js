// ==========================================
// グローバルスコープ（ファイル先頭）
// ==========================================

// --- IndexedDB & Auth Config ---
const DB_NAME = 'ToreKenDB';
const DB_VERSION = 1;
const TOKEN_STORE_NAME = 'auth-tokens';

// --- Authentication Management (with IndexedDB support) ---
class AuthManager {
    static TOKEN_KEY = 'auth_token';
    static EXPIRY_KEY = 'auth_expiry';
    static PERMISSION_KEY = 'auth_permission';

    static async setTokenInDB(token) {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onerror = () => reject("Error opening DB for token storage");
            request.onupgradeneeded = event => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
                    db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
                }
            };
            request.onsuccess = event => {
                const db = event.target.result;
                const transaction = db.transaction([TOKEN_STORE_NAME], 'readwrite');
                const store = transaction.objectStore(TOKEN_STORE_NAME);
                if (token) {
                    store.put({ id: 'auth_token', value: token });
                } else {
                    store.delete('auth_token');
                }
                transaction.oncomplete = () => resolve();
                transaction.onerror = () => reject("Error storing token in DB");
            };
        });
    }

    static async setAuthData(token, expiresIn, permission) {
        localStorage.setItem(this.TOKEN_KEY, token);
        const expiryTime = Date.now() + (expiresIn * 1000);
        localStorage.setItem(this.EXPIRY_KEY, expiryTime.toString());
        localStorage.setItem(this.PERMISSION_KEY, permission);
        try {
            await this.setTokenInDB(token);
            console.log(`Auth token and permission (${permission}) stored. Expires at:`, new Date(expiryTime).toLocaleString());
        } catch (error) {
            console.error("Failed to store token in IndexedDB:", error);
        }
    }

    static getToken() {
        const token = localStorage.getItem(this.TOKEN_KEY);
        const expiry = localStorage.getItem(this.EXPIRY_KEY);
        if (!token || !expiry || Date.now() > parseInt(expiry)) {
            if (token) this.clearAuthData();
            return null;
        }
        return token;
    }

    static getPermission() {
        return localStorage.getItem(this.PERMISSION_KEY);
    }

    static async clearAuthData() {
        localStorage.removeItem(this.TOKEN_KEY);
        localStorage.removeItem(this.EXPIRY_KEY);
        localStorage.removeItem(this.PERMISSION_KEY);
        try {
            await this.setTokenInDB(null);
            console.log('Auth data cleared from localStorage and IndexedDB');
        } catch (error) {
            console.error("Failed to clear token from IndexedDB:", error);
        }
    }

    static isAuthenticated() {
        return this.getToken() !== null;
    }

    static getAuthHeaders() {
        const token = this.getToken();
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    }
}

// --- Helper for VAPID Key ---
function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/-/g, '+')
        .replace(/_/g, '/');

    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}

// --- Authenticated Fetch Wrapper ---
async function fetchWithAuth(url, options = {}) {
    const authHeaders = AuthManager.getAuthHeaders();
    const response = await fetch(url, {
        ...options,
        headers: { ...options.headers, ...authHeaders }
    });

    if (response.status === 401) {
        console.log('Authentication failed (401), redirecting to auth screen');
        await AuthManager.clearAuthData();
        window.dispatchEvent(new CustomEvent('auth-required'));
        throw new Error('Authentication required');
    }
    return response;
}

// --- NotificationManager (from HanaView) ---
class NotificationManager {
    constructor() {
        this.isSupported = 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
        this.vapidPublicKey = null;
    }

    async init() {
        if (!this.isSupported) {
            console.log('Push notifications are not supported');
            return;
        }
        console.log('Initializing NotificationManager...');
        try {
            const response = await fetch('/api/vapid-public-key');
            const data = await response.json();
            this.vapidPublicKey = data.public_key;
            console.log('VAPID public key obtained');
        } catch (error) {
            console.error('Failed to get VAPID public key:', error);
            return;
        }
        const permission = await this.requestPermission();
        if (permission) {
            await this.subscribeUser();
        }
        navigator.serviceWorker.addEventListener('message', event => {
            if (event.data.type === 'data-updated' && event.data.data) {
                console.log('Data updated via push notification');
                // renderAllData not available here, but we can refresh
                // or show notification
                this.showInAppNotification('データが更新されました');
                location.reload();
            }
        });
    }

    async requestPermission() {
        const permission = await Notification.requestPermission();
        console.log('Notification permission:', permission);
        return permission === 'granted';
    }

    async subscribeUser() {
        try {
            const registration = await navigator.serviceWorker.ready;
            let subscription = await registration.pushManager.getSubscription();
            if (!subscription) {
                const convertedVapidKey = this.urlBase64ToUint8Array(this.vapidPublicKey);
                subscription = await registration.pushManager.subscribe({
                    userVisibleOnly: true,
                    applicationServerKey: convertedVapidKey
                });
            }
            await this.sendSubscriptionToServer(subscription);
            if ('sync' in registration) {
                await registration.sync.register('data-sync');
            }
        } catch (error) {
            console.error('Failed to subscribe user:', error);
        }
    }

    async sendSubscriptionToServer(subscription) {
        try {
            if (typeof AuthManager === 'undefined') {
                console.error('❌ AuthManager is not defined yet');
                throw new Error('認証マネージャーが読み込まれていません。');
            }

            if (!AuthManager.isAuthenticated()) {
                console.warn('Cannot register push subscription: not authenticated');
                return;
            }

            console.log('📤 Sending push subscription to server...');

            const response = await fetchWithAuth('/api/subscribe', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(subscription)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Server returned ${response.status}: ${errorText}`);
            }

            const result = await response.json();
            console.log('✅ Push subscription registered:', result);
            this.showInAppNotification(`通知が有効になりました (権限: ${result.permission})`);
        } catch (error) {
            console.error('❌ Error sending subscription to server:', error);
            let errorMessage = error.message || '不明なエラー';
            alert(`⚠️ Push通知の登録に失敗しました:\n${errorMessage}`);
        }
    }

    urlBase64ToUint8Array(base64String) {
        const padding = '='.repeat((4 - base64String.length % 4) % 4);
        const base64 = (base64String + padding)
            .replace(/\-/g, '+')
            .replace(/_/g, '/');
        const rawData = window.atob(base64);
        const outputArray = new Uint8Array(rawData.length);
        for (let i = 0; i < rawData.length; ++i) {
            outputArray[i] = rawData.charCodeAt(i);
        }
        return outputArray;
    }

    showInAppNotification(message) {
        const toast = document.createElement('div');
        toast.className = 'toast-notification';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #006B6B;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 10000;
            animation: slideIn 0.3s ease-out;
        `;
        document.body.appendChild(toast);
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => {
                if (toast.parentNode) document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}

// ==========================================
// DOMContentLoaded以降
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    console.log("Tore-ken App Initializing...");

    // --- DOM Element References ---
    const authContainer = document.getElementById('auth-container');
    const dashboardContainer = document.querySelector('.container');
    const pinInputsContainer = document.getElementById('pin-inputs');
    const pinInputs = pinInputsContainer ? Array.from(pinInputsContainer.querySelectorAll('input')) : [];
    const authErrorMessage = document.getElementById('auth-error-message');
    const authSubmitButton = document.getElementById('auth-submit-button');
    const authLoadingSpinner = document.getElementById('auth-loading');

    // --- State ---
    let failedAttempts = 0;
    const MAX_ATTEMPTS = 5;
    let globalNotificationManager = null;
    let marketHistory = [];
    let currentDateIndex = -1;

    // ✅ 認証エラーイベントのリスナー追加
    window.addEventListener('auth-required', () => {
        showAuthScreen();
    });

    // --- Main App Logic ---
    async function initializeApp() {
        // ✅ 古い認証データのクリーンアップ
        if (localStorage.getItem('auth_token') && !localStorage.getItem('auth_permission')) {
            console.log('🧹 Cleaning old authentication data...');
            await AuthManager.clearAuthData();
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                for (let registration of registrations) {
                    await registration.unregister();
                }
            }
            alert('⚠️ 認証システムが更新されました。再度ログインしてください。');
            location.reload();
            return;
        }

        try {
            if (AuthManager.isAuthenticated()) {
                await showDashboard();
            } else {
                showAuthScreen();
            }
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error('Error during authentication check:', error);
                if (authErrorMessage) authErrorMessage.textContent = 'サーバーとの通信に失敗しました。';
            }
            showAuthScreen();
        }
    }

    async function showDashboard() {
        if (authContainer) authContainer.style.display = 'none';
        if (dashboardContainer) dashboardContainer.style.display = 'block';

        if (typeof AuthManager === 'undefined' || typeof fetchWithAuth === 'undefined') {
            console.error('❌ Required dependencies not loaded. Skipping notification setup.');
            alert('⚠️ アプリの初期化に問題があります。ページを再読み込みしてください。');
            return;
        }

        if (!globalNotificationManager) {
            globalNotificationManager = new NotificationManager();
            try {
                // 少し待機してからNotificationManagerを初期化（iPhone PWA対策）
                await new Promise(resolve => setTimeout(resolve, 100));
                await globalNotificationManager.init();
                console.log('✅ Notifications initialized');
            } catch (error) {
                console.error('❌ Notification initialization failed:', error);
            }
        }

        if (!dashboardContainer.dataset.initialized) {
            console.log("Tore-ken Dashboard Initialized");
            fetchDataAndRender();
            dashboardContainer.dataset.initialized = 'true';
        }
    }

    function showAuthScreen() {
        if (authContainer) authContainer.style.display = 'flex';
        if (dashboardContainer) dashboardContainer.style.display = 'none';
        setupAuthForm();
    }

    function setupAuthForm() {
        if (!pinInputsContainer) return;
        pinInputs.forEach(input => { input.value = ''; input.disabled = false; });
        if(authSubmitButton) authSubmitButton.disabled = false;
        if(authErrorMessage) authErrorMessage.textContent = '';
        failedAttempts = 0;
        pinInputs[0]?.focus();

        pinInputs.forEach((input, index) => {
            input.addEventListener('input', () => {
                if (input.value.length === 1 && index < pinInputs.length - 1) {
                    pinInputs[index + 1].focus();
                }
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                    pinInputs[index - 1].focus();
                }
            });
            input.addEventListener('paste', (e) => {
                e.preventDefault();
                const pasteData = e.clipboardData.getData('text').trim();
                if (/^\d{6}$/.test(pasteData)) {
                    pasteData.split('').forEach((char, i) => { if (pinInputs[i]) pinInputs[i].value = char; });
                    handleAuthSubmit();
                }
            });
        });

        if (authSubmitButton) {
            const newButton = authSubmitButton.cloneNode(true);
            authSubmitButton.parentNode.replaceChild(newButton, authSubmitButton);
            newButton.addEventListener('click', handleAuthSubmit);
        }
    }

    async function handleAuthSubmit() {
        const pin = pinInputs.map(input => input.value).join('');
        if (pin.length !== 6) {
            if (authErrorMessage) authErrorMessage.textContent = '6桁のコードを入力してください。';
            return;
        }
        setLoading(true);
        try {
            const response = await fetch('/api/auth/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pin: pin })
            });
            const data = await response.json();
            if (response.ok && data.success) {
                await AuthManager.setAuthData(data.token, data.expires_in, data.permission);
                console.log('✅ Authentication complete, token saved');
                await showDashboard();
            } else {
                failedAttempts++;
                pinInputs.forEach(input => input.value = '');
                pinInputs[0].focus();
                if (failedAttempts >= MAX_ATTEMPTS) {
                    if (authErrorMessage) authErrorMessage.textContent = '認証に失敗しました。';
                    pinInputs.forEach(input => input.disabled = true);
                    document.getElementById('auth-submit-button').disabled = true;
                } else {
                    if (authErrorMessage) authErrorMessage.textContent = '正しい認証コードを入力してください。';
                }
            }
        } catch (error) {
            console.error('Error during PIN verification:', error);
            if (authErrorMessage) authErrorMessage.textContent = '認証中にエラーが発生しました。';
        } finally {
            setLoading(false);
        }
    }

    function setLoading(isLoading) {
        if (authLoadingSpinner) authLoadingSpinner.style.display = isLoading ? 'block' : 'none';
        const submitBtn = document.getElementById('auth-submit-button');
        if (submitBtn) submitBtn.style.display = isLoading ? 'none' : 'block';
    }

    // --- Dashboard Functions (Refactored) ---

    async function fetchDataAndRender() {
        try {
            const response = await fetchWithAuth('/api/market-analysis');
            if (!response.ok) throw new Error("Failed to load market analysis");

            const data = await response.json();

            if (data.history && data.history.length > 0) {
                marketHistory = data.history;
                currentDateIndex = marketHistory.length - 1; // Default to latest

                // No Plotly rendering needed, image is loaded via HTML
                // Setup Controls
                setupControls();

                // Load Initial Daily Data
                updateDailyView(currentDateIndex);

                // Update Last Updated
                const lastUpdatedEl = document.getElementById('last-updated');
                if (lastUpdatedEl && data.last_updated) {
                    lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`;
                }
            } else {
                console.error("No history data found");
            }
        } catch (error) {
            console.error("Failed to fetch market analysis:", error);
        }
    }

    function setupControls() {
        const slider = document.getElementById('date-slider');
        const prevBtn = document.getElementById('prev-btn');
        const nextBtn = document.getElementById('next-btn');

        slider.min = 0;
        slider.max = marketHistory.length - 1;
        slider.value = currentDateIndex;

        slider.addEventListener('input', (e) => {
            const idx = parseInt(e.target.value);
            updateDailyView(idx);
        });

        prevBtn.addEventListener('click', () => {
            if (currentDateIndex > 0) {
                updateDailyView(currentDateIndex - 1);
                slider.value = currentDateIndex;
            }
        });

        nextBtn.addEventListener('click', () => {
            if (currentDateIndex < marketHistory.length - 1) {
                updateDailyView(currentDateIndex + 1);
                slider.value = currentDateIndex;
            }
        });
    }

    async function updateDailyView(index) {
        currentDateIndex = index;
        const historyItem = marketHistory[index];
        const dateKey = historyItem.date_key; // YYYYMMDD

        // Update Date Display
        document.getElementById('selected-date').textContent = historyItem.date;

        // Update Status Badge
        const badge = document.getElementById('market-status-badge');
        badge.textContent = historyItem.status_text;
        badge.className = 'status-text'; // Reset

        if (historyItem.status_text.includes("Red to")) badge.classList.add('status-green');
        else if (historyItem.status_text.includes("Green to")) badge.classList.add('status-red');
        else if (historyItem.status_text.includes("Green")) badge.classList.add('status-green');
        else if (historyItem.status_text.includes("Red")) badge.classList.add('status-red');
        else badge.classList.add('status-neutral');

        // Move Vertical Line on Image (CSS)
        const chartWrapper = document.getElementById('chart-wrapper');
        const cursor = document.getElementById('chart-cursor');

        if (chartWrapper && cursor && marketHistory.length > 0) {
            // Logic to calculate position
            // The chart image generated by mplfinance has margins.
            // With tight_layout and figsize(10,8), the plot area is roughly 80-90% of width.
            // This is a rough approximation. Ideally we'd know the exact pixel bounds.
            // Let's assume standard margins.

            // Adjust these offsets based on the actual generated image appearance
            const marginLeft = 0.05; // 5% from left
            const marginRight = 0.12; // 12% from right
            const plotWidthPct = 1.0 - marginLeft - marginRight;

            // Calculate percentage position
            // index 0 = left edge of plot area
            // index max = right edge of plot area
            const count = marketHistory.length;
            const pct = (index + 0.5) / count;
            const leftPos = (marginLeft + (pct * plotWidthPct)) * 100;

            cursor.style.left = `${leftPos}%`;
            cursor.style.display = 'block';
        }

        // Fetch Daily Data (Recognition Gap EP ranking)
        const contentDiv = document.getElementById('strong-stocks-content');
        contentDiv.innerHTML = '<div class="loading-spinner-small"></div>';

        try {
            const response = await fetchWithAuth(`/api/daily/${dateKey}`);
            if (response.ok) {
                const data = await response.json();
                renderRecognitionGapRanking(data.recognition_gap_ranking || data.ranking || data.strong_stocks || []);
            } else {
                // Check if this date is 'today' or close enough?
                // Or simply show no data
                contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">Recognition Gap EPランキングはまだありません。</p>';
            }
        } catch (error) {
             contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">ランキングの読み込みに失敗しました。</p>';
        }
    }

    function formatPercentValue(value) {
        if (value === undefined || value === null || value === '') return '-';
        const num = Number(value);
        if (Number.isNaN(num)) return String(value);
        if (Math.abs(num) <= 20) return `${num >= 0 ? '+' : ''}${(num * 100).toFixed(1)}%`;
        return `${num >= 0 ? '+' : ''}${num.toFixed(1)}%`;
    }

    function renderRecognitionGapRanking(stocks) {
        const contentDiv = document.getElementById('strong-stocks-content');
        if (!stocks || stocks.length === 0) {
            contentDiv.innerHTML = '<p style="text-align: center;">Recognition Gap EP候補はありません。</p>';
            return;
        }

        const rows = [...stocks].sort((a, b) => {
            const rankA = Number(a.rank || 9999);
            const rankB = Number(b.rank || 9999);
            return rankA - rankB;
        });

        contentDiv.innerHTML = '';

        const header = document.createElement('div');
        header.style.cssText = 'margin-bottom: 10px; font-size: 0.9em; text-align: right; color: #555;';
        header.textContent = `Recognition Gap EP 7層ランキング: ${rows.length}銘柄`;
        contentDiv.appendChild(header);

        const table = document.createElement('table');
        table.className = 'strong-stocks-table recognition-gap-table';
        table.innerHTML = `
            <thead>
                <tr>
                    <th>順位</th>
                    <th>銘柄</th>
                    <th>Entry</th>
                    <th>含み益</th>
                    <th>State</th>
                    <th>要点</th>
                </tr>
            </thead>
            <tbody></tbody>
        `;
        const tbody = table.querySelector('tbody');

        rows.forEach(s => {
            const tr = document.createElement('tr');
            const summary = s.seven_layer_summary_ja || s.summary_ja || s.summary || '';
            const state = s.thesis_state || '';
            const substate = s.thesis_substate ? ` / ${s.thesis_substate}` : '';

            const appendCell = (text, strong = false) => {
                const td = document.createElement('td');
                if (strong) {
                    const bold = document.createElement('strong');
                    bold.textContent = text;
                    td.appendChild(bold);
                } else {
                    td.textContent = text;
                }
                tr.appendChild(td);
            };

            appendCell(s.rank || '');
            appendCell(s.symbol || s.ticker || '', true);
            appendCell(s.entry_date || s.entry || '-');
            appendCell(formatPercentValue(s.return_since_entry ?? s.return ?? s.gain));
            appendCell(`${state}${substate}`);
            appendCell(summary);
            tbody.appendChild(tr);
        });

        contentDiv.appendChild(table);
    }

    function renderStrongStocks(stocks) {
        const contentDiv = document.getElementById('strong-stocks-content');

        // Filter by ADR% >= 4.0
        const filteredStocks = stocks.filter(s => (s.adr_pct !== undefined && s.adr_pct >= 4.0));

        // Sort by Entry Date (Newest first)
        filteredStocks.sort((a, b) => {
            const dateA = a.entry_date ? new Date(a.entry_date) : new Date(0);
            const dateB = b.entry_date ? new Date(b.entry_date) : new Date(0);
            return dateB - dateA; // Descending
        });

        if (!filteredStocks || filteredStocks.length === 0) {
            contentDiv.innerHTML = '<p style="text-align: center;">No Strong Stocks (ADR% >= 4) found.</p>';
            return;
        }

        // Clear content
        contentDiv.innerHTML = '';

        // Matches Header
        const header = document.createElement('div');
        header.style.cssText = 'margin-bottom: 10px; font-size: 0.9em; text-align: right; color: #555;';
        header.textContent = `Displaying: ${filteredStocks.length} / Total: ${stocks.length}`;
        contentDiv.appendChild(header);

        // Container
        const listContainer = document.createElement('div');
        listContainer.style.cssText = 'display: flex; flex-direction: column; gap: 10px;';

        filteredStocks.forEach(s => {
            // Logic for border styling
            const revAccel = s.revenue_accel === true;
            const epsAccel = s.earnings_accel === true;
            let borderStyle = '2px solid black';

            if (revAccel && epsAccel) {
                borderStyle = '3px solid blue';
            } else if (revAccel || epsAccel) {
                borderStyle = '2px solid blue';
            }

            const item = document.createElement('div');
            item.style.cssText = `border: ${borderStyle}; padding: 10px; font-weight: bold; font-size: 0.7em; background: white;`;

            // Line 1: Ticker, RVol, Price, ADR%
            const line1 = document.createElement('div');

            // Ticker
            const tickerSpan = document.createElement('span');
            tickerSpan.textContent = s.ticker;
            tickerSpan.style.cssText = 'font-size: 1.6em; cursor: pointer; text-decoration: underline; color: #000; margin-right: 15px;';
            line1.appendChild(tickerSpan);

            // RealTime RVol (Magenta)
            const rvolSpan = document.createElement('span');
            rvolSpan.id = `rvol-${s.ticker}`;
            rvolSpan.textContent = `RVol: --`; // Initial placeholder
            rvolSpan.style.cssText = 'color: #FF00FF; font-size: 1.2em; margin-right: 15px;';
            line1.appendChild(rvolSpan);

            // Price
            const priceSpan = document.createElement('span');
            priceSpan.textContent = `Price: $${s.current_price}`;
            priceSpan.style.cssText = 'font-size: 1.2em; color: #333; margin-right: 15px;';
            line1.appendChild(priceSpan);

            // ADR%
            const adrSpan = document.createElement('span');
            adrSpan.textContent = `ADR: ${s.adr_pct}%`;
            adrSpan.style.cssText = 'font-size: 1.2em; color: #333;';
            line1.appendChild(adrSpan);

            item.appendChild(line1);

            // Line 2: RTI & Entry Date
            const line2 = document.createElement('div');
            // Calculate Days Held
            let entryInfo = "";
            if (s.entry_date) {
                const entry = new Date(s.entry_date);
                const today = new Date();
                const diffTime = Math.abs(today - entry);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                entryInfo = `Entry: ${s.entry_date} (${diffDays}d) | `;
            }

            line2.textContent = `${entryInfo}RTI: ${s.rti !== undefined ? s.rti : 'N/A'}`;
            line2.style.fontSize = '1.2em';
            line2.style.marginTop = '5px';
            item.appendChild(line2);

            // Line 3: Fundamentals
            if (s.revenue_display || s.earnings_display) {
                const line3 = document.createElement('div');
                line3.style.cssText = 'font-size: 1.1em; margin-top: 5px; color: #333; display: flex; flex-direction: column;';

                if (s.revenue_display) {
                    const revSpan = document.createElement('span');
                    revSpan.textContent = s.revenue_display;
                    line3.appendChild(revSpan);
                }
                if (s.earnings_display) {
                    const epsSpan = document.createElement('span');
                    epsSpan.textContent = s.earnings_display;
                    line3.appendChild(epsSpan);
                }
                item.appendChild(line3);
            }

            // Chart Container (Hidden by default)
            const chartContainer = document.createElement('div');
            chartContainer.style.display = 'none';
            chartContainer.style.marginTop = '10px';
            chartContainer.style.textAlign = 'center';

            // Toggle Logic
            tickerSpan.addEventListener('click', () => {
                const isHidden = chartContainer.style.display === 'none';
                chartContainer.style.display = isHidden ? 'block' : 'none';

                if (isHidden && !chartContainer.hasChildNodes()) {
                    // Load image
                    if (s.chart_image) {
                        const img = document.createElement('img');
                        img.alt = `${s.ticker} Chart`;
                        img.style.maxWidth = '100%';
                        img.style.border = '1px solid #ccc';
                        img.src = `/api/stock-chart/${s.chart_image}`;

                        // Handle error
                        img.onerror = () => {
                            chartContainer.innerHTML = '<span style="color:red; font-size:0.8em;">Failed to load chart.</span>';
                        };

                        chartContainer.appendChild(img);
                    } else {
                        chartContainer.textContent = 'Chart not available.';
                    }
                }
            });

            item.appendChild(chartContainer);
            listContainer.appendChild(item);
        });

        contentDiv.appendChild(listContainer);

        // Start polling for RealTime RVol if not already started
        if (!window.rvolInterval) {
            fetchRealTimeRVol(); // Initial fetch
            window.rvolInterval = setInterval(fetchRealTimeRVol, 60000); // Poll every 1 minute
        }
    }

    async function fetchRealTimeRVol() {
        try {
            const response = await fetchWithAuth('/api/realtime-rvol');
            if (response.ok) {
                const data = await response.json();
                Object.keys(data).forEach(ticker => {
                    const el = document.getElementById(`rvol-${ticker}`);
                    if (el) {
                        const val = data[ticker];
                        el.textContent = `RVol: ${val.toFixed(2)}x`;
                    }
                });
            }
        } catch (error) {
            console.error("Failed to fetch RealTime RVol:", error);
        }
    }

    // --- Auto Reload Function ---
    function setupAutoReload() {
        // 5-minute Force Reload (PWA)
        setInterval(() => {
            console.log("5-minute force reload triggered");
            location.reload();
        }, 300000); // 300,000 ms = 5 minutes

        const LAST_RELOAD_KEY = 'lastAutoReloadDate';
        setInterval(() => {
            const now = new Date();
            const day = now.getDay();
            const hours = now.getHours();
            const minutes = now.getMinutes();
            const isWeekday = day >= 1 && day <= 5;
            const isReloadTime = hours === 6 && minutes === 30;
            if (isWeekday && isReloadTime) {
                const today = now.toISOString().split('T')[0];
                const lastReloadDate = localStorage.getItem(LAST_RELOAD_KEY);
                if (lastReloadDate !== today) {
                    console.log('Auto-reloading page at 6:30 on a weekday...');
                    localStorage.setItem(LAST_RELOAD_KEY, today);
                    location.reload();
                }
            }
        }, 60000);
    }

    // --- App Initialization ---
    initializeApp();
    setupAutoReload();
});
