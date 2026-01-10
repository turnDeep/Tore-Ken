// ==========================================
// グローバルスコープ（ファイル先頭）
// ==========================================

// --- IndexedDB & Auth Config ---
const DB_NAME = 'HanaViewDB';
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

// --- NotificationManager ---
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
                if (typeof renderAllData === 'function') {
                    renderAllData(event.data.data);
                }
                this.showInAppNotification('データが更新されました');
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
        // AuthManagerが存在するか確認（iPhone PWA対策）
        if (typeof AuthManager === 'undefined') {
            console.error('❌ AuthManager is not defined yet');
            throw new Error('認証マネージャーが読み込まれていません。ページを再読み込みしてください。');
        }

        if (!AuthManager.isAuthenticated()) {
            console.warn('Cannot register push subscription: not authenticated');
            return;
        }

        console.log('📤 Sending push subscription to server...');

        // fetchWithAuthも存在確認（念のため）
        if (typeof fetchWithAuth === 'undefined') {
            console.error('❌ fetchWithAuth is not defined yet');
            throw new Error('通信機能が読み込まれていません。ページを再読み込みしてください。');
        }

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

        // より詳細なエラーメッセージ
        let errorMessage = error.message || '不明なエラー';
        if (error.message.includes('認証マネージャー') || error.message.includes('通信機能')) {
            errorMessage += '\n\niPhone PWAでこの問題が発生する場合：\n1. アプリを完全に終了\n2. Safariでページを開き直す\n3. 再度ホーム画面に追加';
        }

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
                document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}

// ==========================================
// DOMContentLoaded以降
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    console.log("HanaView App Initializing...");

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
            // Service Workerの登録も解除
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                for (let registration of registrations) {
                    await registration.unregister();
                }
            }
            alert('⚠️ 認証システムが更新されました。再度ログインしてください。');
            // ✅ ページをリロードしてService Workerを再登録
            location.reload();
            return; // これ以降の処理を実行しない
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

    function applyTabPermissions() {
        const permission = AuthManager.getPermission();
        const hwb200Tab = document.querySelector('.tab-button[data-tab="hwb200"]');
        const algoTab = document.querySelector('.tab-button[data-tab="algo"]');

        console.log(`Applying permissions for level: ${permission}`);

        if (hwb200Tab) hwb200Tab.style.display = '';
        if (algoTab) algoTab.style.display = '';

        if (permission === 'standard') {
            console.log("Standard permission: Hiding 200MA and Algo tabs.");
            if (hwb200Tab) hwb200Tab.style.display = 'none';
            if (algoTab) algoTab.style.display = 'none';
        } else if (permission === 'secret') {
            console.log("Secret permission: Hiding Algo tab.");
            if (algoTab) algoTab.style.display = 'none';
        } else if (permission === 'ura') {
            console.log("Ura permission: All tabs visible.");
        }
    }

async function showDashboard() {
    if (authContainer) authContainer.style.display = 'none';
    if (dashboardContainer) dashboardContainer.style.display = 'block';

    applyTabPermissions();

    // NotificationManager初期化前に必要な依存関係を確認
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
            alert('⚠️ Push通知の登録に失敗しました。ページを再読み込みしてください。');
        }
    }

    if (!dashboardContainer.dataset.initialized) {
        console.log("HanaView Dashboard Initialized");
        initTabs();
        fetchDataAndRender();
        initSwipeNavigation();

        if (document.getElementById('hwb200-content')) {
            initHWB200MA();
        }

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

    // --- Dashboard Functions ---

    async function fetchDataAndRender() {
        try {
            const response = await fetchWithAuth('/api/data');
            const data = await response.json();
            renderAllData(data);
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error("Failed to fetch data:", error);
                document.getElementById('dashboard-content').innerHTML =
                    `<div class="card"><p>データの読み込みに失敗しました: ${error.message}</p></div>`;
            }
        }
    }

    // --- Tab-switching logic ---
    function initTabs() {
        const tabContainer = document.querySelector('.tab-container');
        tabContainer.addEventListener('click', (e) => {
            const button = e.target.closest('.tab-button');
            if (!button) return;

            const targetTab = button.dataset.tab;

            document.querySelectorAll('.tab-button').forEach(b => {
                const isActive = b.dataset.tab === targetTab;
                b.classList.toggle('active', isActive);
            });

            document.querySelectorAll('.tab-pane').forEach(p => {
                const isActive = p.id === `${targetTab}-content`;
                p.classList.toggle('active', isActive);
            });

            if (targetTab === 'hwb200' && window.hwb200Manager) {
                window.hwb200Manager.loadData();
            }

            setTimeout(() => window.scrollTo(0, 0), 0);
        });
    }

    // --- HWB 200MA Manager ---
    function initHWB200MA() {
        window.hwb200Manager = new HWB200MAManager();
        console.log('HWB200MAManager initialized');
    }

    class HWB200MAManager {
        constructor() {
            this.summaryData = null;
            this.currentView = 'summary';
            this.activeListType = 'signal_today'; // 初期値は当日ブレイクアウト
            this.initEventListeners();
        }

        initEventListeners() {
            const searchBtn = document.getElementById('hwb-analyze-btn');
            if (searchBtn) {
                searchBtn.addEventListener('click', () => {
                    if (searchBtn.dataset.state === 'reset') {
                        this.resetToSummary();
                    } else {
                        this.searchTicker();
                    }
                });
            }
        }

        async searchTicker() {
            const input = document.getElementById('hwb-ticker-input');
            const ticker = input.value.trim().toUpperCase();

            if (!ticker) {
                this.showStatus('ティッカーシンボルを入力してください', 'warning');
                return;
            }

            this.showStatus(`${ticker}のデータを検索中...`, 'info');

            try {
                const response = await fetchWithAuth(`/api/hwb/symbols/${ticker}`);

                if (!response.ok) {
                    if (response.status === 404) {
                        this.showStatus(`❌ ${ticker}はスキャン対象外またはシグナルなし`, 'warning');
                        return;
                    }
                    throw new Error(`検索に失敗しました: ${response.status}`);
                }

                const symbolData = await response.json();

                this.currentView = 'search';
                this.renderSearchResults(ticker, symbolData);

                const searchBtn = document.getElementById('hwb-analyze-btn');
                if (searchBtn) {
                    searchBtn.textContent = 'リセット';
                    searchBtn.dataset.state = 'reset';
                }

                this.showStatus(`✅ ${ticker}の検索結果を表示中`, 'info');

            } catch (error) {
                console.error('Search error:', error);
                this.showStatus(`❌ エラー: ${error.message}`, 'error');
            }
        }

        resetToSummary() {
            this.currentView = 'summary';
            const input = document.getElementById('hwb-ticker-input');
            if (input) input.value = '';

            const searchBtn = document.getElementById('hwb-analyze-btn');
            if (searchBtn) {
                searchBtn.textContent = '検索';
                searchBtn.dataset.state = 'search';
            }

            this.render();

            const { updated_at, summary } = this.summaryData;
            const displayDate = updated_at ? formatDateForDisplay(updated_at) : this.summaryData.scan_date;
            
            const todayCount = summary.signals_today?.length || 0;
            const recentCount = summary.signals_recent?.length || 0;
            const candidatesCount = summary.candidates?.length || 0;
            
            this.showStatus(
                `最終更新: ${displayDate} | 当日: ${todayCount} | 直近: ${recentCount} | 監視: ${candidatesCount}`,
                'info'
            );
        }

        renderSearchResults(ticker, symbolData) {
            const container = document.getElementById('hwb-content');
            container.innerHTML = '';

            const resultDiv = document.createElement('div');
            resultDiv.className = 'hwb-search-results';

            const signals = symbolData.signals || [];

            if (signals.length === 0) {
                resultDiv.innerHTML = `
                    <div class="hwb-summary">
                        <h2>${ticker} の検索結果</h2>
                        <p class="info-message">このシンボルにはブレイクアウトシグナルがありません。</p>
                    </div>
                `;
                container.appendChild(resultDiv);
                return;
            }

            // シグナル日付でグループ化
            const signalsByDate = {};
            signals.forEach(signal => {
                const date = signal.breakout_date;
                if (!signalsByDate[date]) {
                    signalsByDate[date] = [];
                }
                signalsByDate[date].push(signal);
            });

            resultDiv.innerHTML = `
                <div class="hwb-summary">
                    <h2>${ticker} の検索結果</h2>
                    <div class="scan-info">
                        ブレイクアウトシグナル: ${signals.length}件
                    </div>
                </div>
            `;

            // 日付ごとにセクション作成
            const sortedDates = Object.keys(signalsByDate).sort().reverse();

            sortedDates.forEach(date => {
                const section = document.createElement('div');
                section.className = 'hwb-charts-section';
                section.innerHTML = `<h2>📅 ${date}</h2>`;

                const list = document.createElement('div');
                list.className = 'hwb-symbol-list';

                signalsByDate[date].forEach(signal => {
                    const item = document.createElement('div');
                    item.className = 'hwb-symbol-item';

                    // 日付フォーマットを修正（T00:00:00を削除）
                    const dateInfo = signal.breakout_date ? signal.breakout_date.split('T')[0] : '';

                    // RS Ratingの表示
                    let rsRatingHtml = '';
                    if (signal.rs_rating !== undefined && signal.rs_rating !== null) {
                        const rsClass = this.getRSClass(signal.rs_rating);
                        rsRatingHtml = `<span class="hwb-rs-badge ${rsClass}">RS ${signal.rs_rating}</span>`;
                    }

                    // 出来高増加率の表示
                    let volumeHtml = '';
                    if (signal.volume_increase_pct !== undefined && signal.volume_increase_pct !== null) {
                        const volClass = this.getVolumeClass(signal.volume_increase_pct);
                        volumeHtml = `<span class="hwb-volume-badge ${volClass}">Vol +${signal.volume_increase_pct}%</span>`;
                    }

                    item.innerHTML = `
                        <span class="hwb-symbol-name">${ticker}</span>
                        ${rsRatingHtml}
                        ${volumeHtml}
                        <span class="hwb-symbol-date">${dateInfo}</span>
                    `;
                    list.appendChild(item);
                });

                section.appendChild(list);
                resultDiv.appendChild(section);
            });

            container.appendChild(resultDiv);
        }

        async loadData() {
            this.showStatus('最新のサマリーを読み込み中...', 'info');
            try {
                const response = await fetchWithAuth('/api/hwb/daily/latest');
                if (!response.ok) {
                    if (response.status === 404) {
                        this.showStatus('データがありません。スキャンを実行してください。', 'warning');
                        document.getElementById('hwb-content').innerHTML =
                            '<div class="card"><p>データがありません。スキャンを実行してください。</p></div>';
                    } else {
                        throw new Error(`サーバーエラー: ${response.status}`);
                    }
                    return;
                }

                this.summaryData = await response.json();
                this.currentView = 'summary';
                this.render();

                const { updated_at, summary } = this.summaryData;
                const displayDate = updated_at ? formatDateForDisplay(updated_at) : this.summaryData.scan_date;
                
                const todayCount = summary.signals_today?.length || 0;
                const recentCount = summary.signals_recent?.length || 0;
                const candidatesCount = summary.candidates?.length || 0;
                
                this.showStatus(
                    `最終更新: ${displayDate} | 当日: ${todayCount} | 直近: ${recentCount} | 監視: ${candidatesCount}`,
                    'info'
                );

            } catch (error) {
                console.error('HWB summary loading error:', error);
                this.showStatus(`❌ データ読み込みエラー: ${error.message}`, 'error');
            }
        }

        render() {
            if (!this.summaryData) return;
            const container = document.getElementById('hwb-content');
            container.innerHTML = '';

            this.renderSummary(container);
            this.renderLists(container);
        }

        renderSummary(container) {
            const { updated_at, scan_date, scan_time, total_scanned, summary } = this.summaryData;
            const summaryDiv = document.createElement('div');
            summaryDiv.className = 'hwb-summary';
            const displayDate = updated_at ? formatDateForDisplay(updated_at) : `${scan_date} ${scan_time}`;

            const todayCount = summary.signals_today?.length || 0;
            const recentCount = summary.signals_recent?.length || 0;
            const candidatesCount = summary.candidates?.length || 0;

            summaryDiv.innerHTML = `
                <h2>200MAシステム</h2>
                <div class="scan-info">
                    データ更新: ${displayDate} | 処理銘柄: ${total_scanned}
                </div>
                <div class="hwb-summary-grid">
                    <div class="summary-card ${this.activeListType === 'signal_today' ? 'active' : ''}" data-list-type="signal_today">
                        <h3>当日ブレイクアウト</h3>
                        <p class="summary-count">${todayCount}</p>
                    </div>
                    <div class="summary-card ${this.activeListType === 'signal_recent' ? 'active' : ''}" data-list-type="signal_recent">
                        <h3>直近5営業日</h3>
                        <p class="summary-count">${recentCount}</p>
                    </div>
                    <div class="summary-card ${this.activeListType === 'candidate' ? 'active' : ''}" data-list-type="candidate">
                        <h3>監視銘柄</h3>
                        <p class="summary-count">${candidatesCount}</p>
                    </div>
                </div>
            `;

            // クリックイベントを追加
            const cards = summaryDiv.querySelectorAll('.summary-card');
            cards.forEach(card => {
                card.style.cursor = 'pointer';
                card.addEventListener('click', () => {
                    const listType = card.dataset.listType;
                    this.activeListType = listType;
                    this.refreshView();
                });
            });

            container.appendChild(summaryDiv);
        }

        renderLists(container) {
            const { signals_today = [], signals_recent = [], candidates = [] } = this.summaryData.summary;

            // activeListTypeに応じて表示するリストを切り替え
            if (this.activeListType === 'signal_today' && signals_today.length > 0) {
                this.renderSymbolList(container, '当日ブレイクアウト', signals_today, 'signal_today');
            } else if (this.activeListType === 'signal_recent' && signals_recent.length > 0) {
                this.renderSymbolList(container, '直近5営業日以内', signals_recent, 'signal_recent');
            } else if (this.activeListType === 'candidate' && candidates.length > 0) {
                this.renderSymbolList(container, '監視銘柄', candidates, 'candidate');
            }
        }

        // ビューをリフレッシュするメソッドを追加
        refreshView() {
            const container = document.getElementById('hwb-content');
            if (container && this.summaryData) {
                container.innerHTML = '';
                this.renderSummary(container);
                this.renderLists(container);
            }
        }

// renderSymbolListメソッドを修正
renderSymbolList(container, title, items, type) {
    const section = document.createElement('div');
    section.className = 'hwb-symbol-section';
    section.innerHTML = `<h2>${title}</h2>`;

    const list = document.createElement('div');
    list.className = 'hwb-symbol-list';

    // RS rating降順でソート（当日ブレイクアウトと直近5営業日の場合）
    let sortedItems = [...items];
    if (type === 'signal_today' || type === 'signal_recent') {
        sortedItems.sort((a, b) => {
            const rsA = a.rs_rating !== undefined && a.rs_rating !== null ? a.rs_rating : -1;
            const rsB = b.rs_rating !== undefined && b.rs_rating !== null ? b.rs_rating : -1;
            return rsB - rsA; // 降順
        });
    }

    sortedItems.forEach(item => {
        const symbolItem = document.createElement('div');
        symbolItem.className = 'hwb-symbol-item';

        let dateInfo = '';
        let rsRatingHtml = '';

        let volumeHtml = '';

        if (type === 'signal_today' || type === 'signal_recent') {
            // 日付フォーマットを変更（T00:00:00を削除）
            dateInfo = item.signal_date ? item.signal_date.split('T')[0] : '';

            // RS Ratingの表示
            if (item.rs_rating !== undefined && item.rs_rating !== null) {
                const rsClass = this.getRSClass(item.rs_rating);
                rsRatingHtml = `<span class="hwb-rs-badge ${rsClass}">RS ${item.rs_rating}</span>`;
            }

            // 出来高増加率の表示
            if (item.volume_increase_pct !== undefined && item.volume_increase_pct !== null) {
                const volClass = this.getVolumeClass(item.volume_increase_pct);
                const sign = item.volume_increase_pct > 0 ? '+' : '';
                volumeHtml = `<span class="hwb-volume-badge ${volClass}">Vol ${sign}${item.volume_increase_pct}%</span>`;
            }
        } else {
            // 監視銘柄の日付フォーマットも変更
            dateInfo = item.fvg_date ? item.fvg_date.split('T')[0] : '';
        }

        // Chart Image
        const chartUrl = `charts/${item.symbol}.png?v=${new Date().getTime()}`;
        symbolItem.innerHTML = `
            <div class="hwb-symbol-header" style="width: 100%;">
                 <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                    <span class="hwb-symbol-name">${item.symbol}</span>
                    <span class="hwb-symbol-date">${dateInfo}</span>
                 </div>
                 <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="display: flex; align-items: center;">${rsRatingHtml}</div>
                    <div style="display: flex; align-items: center;">${volumeHtml}</div>
                 </div>
            </div>
            <div class="hwb-symbol-chart" style="width: 100%; margin-top: 10px;">
                <img src="${chartUrl}" alt="${item.symbol} Chart" class="hwb-chart-img" style="width: 100%; height: auto; border-radius: 4px;" loading="lazy" onerror="this.style.display='none'">
            </div>
        `;
        // Flex direction needs to be column for this layout
        symbolItem.style.flexDirection = 'column';
        symbolItem.style.alignItems = 'flex-start';

        list.appendChild(symbolItem);

        // Add Double-Tap Listener for the newly added image
        const img = symbolItem.querySelector('.hwb-chart-img');
        if (img) {
            this.addDoubleTapListener(img, chartUrl);
        }
    });

    section.appendChild(list);
    container.appendChild(section);
}

// ✅ 画像のダブルタップイベントリスナー追加
addDoubleTapListener(element, imageUrl) {
    let lastTap = 0;

    element.addEventListener('touchend', (e) => {
        const currentTime = new Date().getTime();
        const tapLength = currentTime - lastTap;
        if (tapLength < 500 && tapLength > 0) {
            e.preventDefault(); // Prevent zoom
            this.showImagePopup(imageUrl);
        }
        lastTap = currentTime;
    });

    // For desktop testing
    element.addEventListener('dblclick', () => {
        this.showImagePopup(imageUrl);
    });
}

// ✅ 画像ポップアップ表示
showImagePopup(imageUrl) {
    // Create overlay
    const overlay = document.createElement('div');
    overlay.className = 'image-popup-overlay';

    // Create image
    const img = document.createElement('img');
    img.src = imageUrl;
    img.className = 'image-popup-content';

    // Prevent closing when clicking the image itself
    img.addEventListener('click', (e) => {
        e.stopPropagation();
    });

    // Close on overlay click
    overlay.addEventListener('click', () => {
        document.body.removeChild(overlay);
    });

    overlay.appendChild(img);
    document.body.appendChild(overlay);
}

// ✅ RS Ratingの色分けメソッドを追加
getRSClass(rsRating) {
    if (rsRating >= 90) return 'rs-excellent';  // 緑
    if (rsRating >= 80) return 'rs-good';       // 青
    if (rsRating >= 70) return 'rs-average';    // 黄
    return 'rs-weak';                           // 灰色
}

// ✅ 出来高増加率の色分けメソッドを追加
getVolumeClass(volumeIncreasePct) {
    if (volumeIncreasePct >= 127) return 'vol-strong';     // 濃い青
    if (volumeIncreasePct >= 50) return 'vol-moderate';    // 薄い青
    return 'vol-weak';                                     // 灰色
}

        showStatus(message, type = 'info') {
            const statusDiv = document.getElementById('hwb-status');
            if (statusDiv) {
                statusDiv.textContent = message;
                statusDiv.className = `hwb-status-info ${type}`;
            }
        }
    }

    // --- Existing rendering functions ---
    function formatDateForDisplay(dateInput) {
        if (!dateInput) return '';
        try {
            const date = new Date(dateInput);
            if (isNaN(date.getTime())) return '';
            return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
        } catch (e) { return ''; }
    }

    function renderLightweightChart(containerId, data, title) {
        const container = document.getElementById(containerId);
        if (!container || !data || data.length === 0) {
            container.innerHTML = `<p>Chart data for ${title} is not available.</p>`;
            return;
        }
        container.innerHTML = '';

        const chart = LightweightCharts.createChart(container, {
            width: container.clientWidth,
            height: 300,
            layout: {
                backgroundColor: '#ffffff',
                textColor: '#333333'
            },
            grid: {
                vertLines: { color: '#e1e1e1' },
                horzLines: { color: '#e1e1e1' }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal
            },
            timeScale: {
                borderColor: '#cccccc',
                timeVisible: true,
                secondsVisible: false
            },
            handleScroll: false,
            handleScale: false
        });

        const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a'
        });

        const chartData = data.map(item => ({
            time: (new Date(item.time).getTime() / 1000),
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close
        }));

        candlestickSeries.setData(chartData);
        chart.timeScale().fitContent();

        new ResizeObserver(entries => {
            if (entries.length > 0 && entries[0].contentRect.width > 0) {
                chart.applyOptions({ width: entries[0].contentRect.width });
            }
        }).observe(container);
    }

    function renderMarketOverview(container, marketData, lastUpdated) {
        if (!container) return;
        container.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'card';
        let content = '';
        if (marketData.fear_and_greed) { content += `<div class="market-section"><h3>Fear & Greed Index</h3><div class="fg-container" style="display: flex; justify-content: center; align-items: center; min-height: 400px;"><img src="/fear_and_greed_gauge.png?v=${new Date().getTime()}" alt="Fear and Greed Index Gauge" style="max-width: 100%; height: auto;"></div></div>`; }
        content += `<div class="market-grid"><div class="market-section"><h3>VIX (4h足)</h3><div class="chart-container" id="vix-chart-container"></div></div><div class="market-section"><h3>米国10年債金利 (4h足)</h3><div class="chart-container" id="t-note-chart-container"></div></div></div>`;
        if (marketData.ai_commentary) { const dateHtml = formatDateForDisplay(lastUpdated) ? `<p class="ai-date">${formatDateForDisplay(lastUpdated)}</p>` : ''; content += `<div class="market-section"><div class="ai-header"><h3>AI解説</h3>${dateHtml}</div><p>${marketData.ai_commentary.replace(/\n/g, '<br>')}</p></div>`; }
        card.innerHTML = content;
        container.appendChild(card);
        if (marketData.vix && marketData.vix.history) { renderLightweightChart('vix-chart-container', marketData.vix.history, 'VIX'); }
        if (marketData.t_note_future && marketData.t_note_future.history) { renderLightweightChart('t-note-chart-container', marketData.t_note_future.history, '10y T-Note'); }
    }

    function renderNews(container, newsData, lastUpdated) {
        if (!container || !newsData || (!newsData.summary && (!newsData.topics || newsData.topics.length === 0))) { container.innerHTML = '<div class="card"><p>ニュースデータがありません。</p></div>'; return; }
        container.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'card news-card';
        if (newsData.summary) {
            const summaryContainer = document.createElement('div');
            summaryContainer.className = 'news-summary';
            const summaryHeader = document.createElement('div');
            summaryHeader.className = 'news-summary-header';
            let title = '<h3>今朝のサマリー</h3>';
            let dateString = '';
            if (lastUpdated) { const date = new Date(lastUpdated); if (date.getDay() === 1) title = '<h3>先週のサマリー</h3>'; dateString = `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${date.getHours()}:${String(date.getMinutes()).padStart(2, '0')}`; }
            summaryHeader.innerHTML = `${title}<p class="summary-date">${dateString}</p>`;
            const summaryBody = document.createElement('div');
            summaryBody.className = 'news-summary-body';
            summaryBody.innerHTML = `<p>${newsData.summary.replace(/\n/g, '<br>')}</p><img src="icons/suit.PNG" alt="suit" class="summary-image">`;
            summaryContainer.appendChild(summaryHeader);
            summaryContainer.appendChild(summaryBody);
            card.appendChild(summaryContainer);
        }
        if (newsData.topics && newsData.topics.length > 0) {
            const topicsOuterContainer = document.createElement('div');
            topicsOuterContainer.className = 'main-topics-outer-container';
            topicsOuterContainer.innerHTML = '<h3>主要トピック</h3>';
            const topicsContainer = document.createElement('div');
            topicsContainer.className = 'main-topics-container';
            newsData.topics.forEach((topic, index) => {
                const topicBox = document.createElement('div');
                topicBox.className = `topic-box topic-${index + 1}`;
                const topicContent = topic.analysis ? `<p>${topic.analysis.replace(/\n/g, '<br>')}</p>` : `<p>${topic.body}</p>`;
                const sourceIcon = `<a href="${topic.url}" target="_blank" class="source-link"><img src="${topic.source_icon_url || 'icons/external-link.svg'}" alt="Source" class="source-icon" onerror="this.onerror=null;this.src='icons/external-link.svg';"></a>`;
                topicBox.innerHTML = `<div class="topic-number-container"><div class="topic-number">${index + 1}</div></div><div class="topic-details"><p class="topic-title">${topic.title}</p><div class="topic-content">${topicContent}${sourceIcon}</div></div>`;
                topicsContainer.appendChild(topicBox);
            });
            topicsOuterContainer.appendChild(topicsContainer);
            card.appendChild(topicsOuterContainer);
        }
        container.appendChild(card);
    }

    function getPerformanceColor(p) { if (p >= 3) return '#00c853'; if (p > 1) return '#66bb6a'; if (p > 0) return '#2e7d32'; if (p == 0) return '#888888'; if (p > -1) return '#e53935'; if (p > -3) return '#ef5350'; return '#c62828'; }

    function renderGridHeatmap(container, title, heatmapData) {
        if (!container) return;
        container.innerHTML = '';
        let items = heatmapData?.items || heatmapData?.stocks || [];
        const isSP500 = title.includes('SP500');
        if (items.length > 0) {
            if (isSP500) { const stocks = items.filter(d => d.market_cap).sort((a, b) => b.market_cap - a.market_cap).slice(0, 30); const etfs = items.filter(d => !d.market_cap); items = [...stocks, ...etfs]; }
            else { items.sort((a, b) => b.market_cap - a.market_cap); items = items.slice(0, 30); }
        }
        if (items.length === 0) return;
        const card = document.createElement('div');
        card.className = 'card';
        const heatmapWrapper = document.createElement('div');
        heatmapWrapper.className = 'heatmap-wrapper';
        heatmapWrapper.innerHTML = `<h2 class="heatmap-main-title">${title}</h2>`;
        const itemsPerRow = 6, margin = { top: 10, right: 10, bottom: 10, left: 10 }, containerWidth = container.clientWidth || 1000, width = containerWidth - margin.left - margin.right, tilePadding = 5, tileWidth = (width - (itemsPerRow - 1) * tilePadding) / itemsPerRow, tileHeight = tileWidth, etfGap = isSP500 ? tileHeight * 0.5 : 0;
        let yPos = 0; const yPositions = [];
        for (let i = 0; i < items.length; i++) { if (isSP500 && i === items.findIndex(d => !d.market_cap)) { if (i % itemsPerRow !== 0) yPos += tileHeight + tilePadding; yPos += etfGap; } yPositions.push(yPos); if ((i + 1) % itemsPerRow === 0 && i + 1 < items.length) yPos += tileHeight + tilePadding; }
        const totalHeight = yPos + tileHeight;
        const svg = d3.create("svg").attr("viewBox", `0 0 ${containerWidth} ${totalHeight + margin.top + margin.bottom}`).attr("width", "100%").attr("height", "auto");
        const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
        const tooltip = d3.select("body").append("div").attr("class", "heatmap-tooltip").style("opacity", 0);
        const nodes = g.selectAll("g").data(items).enter().append("g").attr("transform", (d, i) => `translate(${(i % itemsPerRow) * (tileWidth + tilePadding)},${yPositions[i]})`);
        nodes.append("rect").attr("width", tileWidth).attr("height", tileHeight).attr("fill", d => getPerformanceColor(d.performance)).on("mouseover", (e, d) => { tooltip.transition().duration(200).style("opacity", .9); tooltip.html(`<strong>${d.ticker}</strong><br/>Perf: ${d.performance.toFixed(2)}%`).style("left", `${e.pageX + 5}px`).style("top", `${e.pageY - 28}px`); }).on("mouseout", () => tooltip.transition().duration(500).style("opacity", 0));
        const text = nodes.append("text").attr("class", "stock-label").attr("x", tileWidth / 2).attr("y", tileHeight / 2).attr("text-anchor", "middle").attr("dominant-baseline", "central").style("pointer-events", "none");
        text.append("tspan").attr("class", "ticker-label").style("font-size", `${Math.max(10, Math.min(tileWidth / 3, 24)) * 1.5}px`).text(d => d.ticker);
        text.append("tspan").attr("class", "performance-label").attr("x", tileWidth / 2).attr("dy", "1.2em").style("font-size", `${Math.max(8, Math.min(tileWidth / 4, 18)) * 1.5}px`).text(d => `${d.performance.toFixed(2)}%`);
        heatmapWrapper.appendChild(svg.node());
        card.appendChild(heatmapWrapper);
        container.appendChild(card);
    }

    function renderIndicators(container, indicatorsData) {
        if (!container) return;
        container.innerHTML = '';
        const { economic = [], us_earnings = [], jp_earnings = [], economic_commentary, earnings_commentary } = indicatorsData || {};
        const economicCard = document.createElement('div');
        economicCard.className = 'card';
        economicCard.innerHTML = '<h3>経済指標カレンダー (重要度★★以上)</h3>';
        const relevantIndicators = economic.filter(ind => (ind.importance?.match(/★/g) || []).length >= 2);
        if (relevantIndicators.length > 0) { const table = document.createElement('table'); table.className = 'indicators-table'; table.innerHTML = `<thead><tr><th>発表日</th><th>発表時刻</th><th>指標名</th><th>重要度</th><th>前回</th><th>予測</th></tr></thead>`; const tbody = document.createElement('tbody'); relevantIndicators.forEach(ind => { const row = document.createElement('tr'); const [date, time] = (ind.datetime || ' / ').split(' '); row.innerHTML = `<td>${date||'--'}</td><td>${time||'--'}</td><td>${ind.name||'--'}</td><td class="importance-${(ind.importance.match(/★/g)||[]).length}">${ind.importance}</td><td>${ind.previous||'--'}</td><td>${ind.forecast||'--'}</td>`; tbody.appendChild(row); }); table.appendChild(tbody); economicCard.appendChild(table); } else { economicCard.innerHTML += '<p>予定されている重要経済指標はありません。</p>'; }
        if (economic_commentary) { const div = document.createElement('div'); div.className = 'ai-commentary'; div.innerHTML = `<div class="ai-header"><h3>AI解説</h3></div><p>${economic_commentary.replace(/\n/g, '<br>')}</p>`; economicCard.appendChild(div); }
        container.appendChild(economicCard);
        const allEarnings = [...us_earnings, ...jp_earnings].sort((a,b) => (a.datetime||'').localeCompare(b.datetime||''));
        const earningsCard = document.createElement('div');
        earningsCard.className = 'card';
        earningsCard.innerHTML = '<h3>注目決算</h3>';
        if (allEarnings.length > 0) { const table = document.createElement('table'); table.className = 'indicators-table'; table.innerHTML = `<thead><tr><th>発表日時</th><th>ティッカー</th><th>企業名</th></tr></thead>`; const tbody = document.createElement('tbody'); allEarnings.forEach(e => { const row = document.createElement('tr'); row.innerHTML = `<td>${e.datetime||'--'}</td><td>${e.ticker||'--'}</td><td>${e.company||''}</td>`; tbody.appendChild(row); }); table.appendChild(tbody); earningsCard.appendChild(table); } else { earningsCard.innerHTML += '<p>予定されている注目決算はありません。</p>'; }
        if (earnings_commentary) { const div = document.createElement('div'); div.className = 'ai-commentary'; div.innerHTML = `<div class="ai-header"><h3>AI解説</h3></div><p>${earnings_commentary.replace(/\n/g, '<br>')}</p>`; earningsCard.appendChild(div); }
        container.appendChild(earningsCard);
    }

    function renderColumn(container, columnData) {
        if (!container) return;
        container.innerHTML = '';
        if (typeof columnData === 'string') { container.innerHTML = `<div class="card"><p>${columnData}</p></div>`; return; }
        const report = columnData ? (columnData.daily_report || columnData.weekly_report) : null;
        if (report && report.content) { const card = document.createElement('div'); card.className = 'card'; const dateHtml = formatDateForDisplay(report.date) ? `<p class="ai-date">${formatDateForDisplay(report.date)}</p>` : ''; card.innerHTML = `<div class="column-container"><div class="ai-header"><h3>${report.title || 'AI解説'}</h3>${dateHtml}</div><div class="column-content">${report.content.replace(/\n/g, '<br>')}</div></div>`; container.appendChild(card); }
        else { container.innerHTML = `<div class="card"><p>${report && report.error ? '生成が失敗しました。' : 'AI解説はまだありません。（月曜日に週間分、火〜金曜日に当日分が生成されます）'}</p></div>`; }
    }

    function renderHeatmapCommentary(container, commentary, lastUpdated) {
        if (!container || !commentary) return;
        const card = document.createElement('div');
        card.className = 'card';
        const dateHtml = formatDateForDisplay(lastUpdated) ? `<p class="ai-date">${formatDateForDisplay(lastUpdated)}</p>` : '';
        card.innerHTML = `<div class="ai-commentary"><div class="ai-header"><h3>AI解説</h3>${dateHtml}</div><p>${commentary.replace(/\n/g, '<br>')}</p></div>`;
        container.appendChild(card);
    }

    function renderAllData(data) {
        console.log("Rendering all data:", data);
        const lastUpdatedEl = document.getElementById('last-updated');
        if (lastUpdatedEl && data.last_updated) { lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`; }
        renderMarketOverview(document.getElementById('market-content'), data.market, data.last_updated);
        renderNews(document.getElementById('news-content'), data.news, data.last_updated);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1d'), 'Nasdaq (1-Day)', data.nasdaq_heatmap_1d);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1w'), 'Nasdaq (1-Week)', data.nasdaq_heatmap_1w);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1m'), 'Nasdaq (1-Month)', data.nasdaq_heatmap_1m);
        renderHeatmapCommentary(document.getElementById('nasdaq-commentary'), data.nasdaq_heatmap?.ai_commentary, data.last_updated);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1d'), 'SP500 & Sector ETFs (1-Day)', data.sp500_combined_heatmap_1d);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1w'), 'SP500 & Sector ETFs (1-Week)', data.sp500_combined_heatmap_1w);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1m'), 'SP500 & Sector ETFs (1-Month)', data.sp500_combined_heatmap_1m);
        renderHeatmapCommentary(document.getElementById('sp500-commentary'), data.sp500_heatmap?.ai_commentary, data.last_updated);
        renderIndicators(document.getElementById('indicators-content'), data.indicators, data.last_updated);
        renderColumn(document.getElementById('column-content'), data.column);
    }

    // --- Swipe Navigation ---
    function initSwipeNavigation() {
        const contentArea = document.getElementById('dashboard-content');
        let touchstartX = 0;
        let touchstartY = 0;
        let hasScrolledVertically = false;
        const verticalScrollThreshold = 10;
        const horizontalSwipeThreshold = 100;
        contentArea.addEventListener('touchstart', e => {
            touchstartX = e.touches[0].screenX;
            touchstartY = e.touches[0].screenY;
            hasScrolledVertically = false;
        }, { passive: true });
        contentArea.addEventListener('touchmove', e => {
            if (hasScrolledVertically) return;
            const touchmoveY = e.touches[0].screenY;
            const deltaY = Math.abs(touchmoveY - touchstartY);
            if (deltaY > verticalScrollThreshold) {
                hasScrolledVertically = true;
            }
        }, { passive: true });
        contentArea.addEventListener('touchend', e => {
            if (hasScrolledVertically) return;
            const touchendX = e.changedTouches[0].screenX;
            const deltaX = touchendX - touchstartX;
            if (Math.abs(deltaX) > horizontalSwipeThreshold) {
                const tabButtons = Array.from(document.querySelectorAll('.tab-button'))
                    .filter(btn => btn.style.display !== 'none');
                const currentIndex = tabButtons.findIndex(b => b.classList.contains('active'));

                if (currentIndex === -1) return;

                let nextIndex = (deltaX > 0) ? currentIndex - 1 : currentIndex + 1;
                if (nextIndex < 0) {
                    nextIndex = tabButtons.length - 1;
                } else if (nextIndex >= tabButtons.length) {
                    nextIndex = 0;
                }
                if (tabButtons[nextIndex]) {
                    tabButtons[nextIndex].click();
                }
            }
        }, { passive: true });
    }

    // --- Auto Reload Function ---
    function setupAutoReload() {
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

// ==========================================
// Algo Manager Implementation
// ==========================================
class AlgoManager {
    constructor() {
        this.summaryData = null;
        this.currentView = 'summary';
        this.activeScreener = 'momentum_97'; // Default screener
        this.activePortfolio = 'aggressive'; // Default portfolio
        this.initEventListeners();
    }

    initEventListeners() {
        const screenerButtons = document.querySelectorAll('.screener-btn');
        screenerButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                this.activeScreener = btn.dataset.screener;
                this.switchScreener(btn.dataset.screener);
            });
        });

        const searchBtn = document.getElementById('algo-analyze-btn');
        if (searchBtn) {
            searchBtn.addEventListener('click', () => {
                if (searchBtn.dataset.state === 'reset') {
                    this.resetToSummary();
                } else {
                    this.searchTicker();
                }
            });
        }
    }

    async loadData() {
        this.showStatus('最新のAlgoデータを読み込み中...', 'info');

        try {
            const response = await fetchWithAuth('/api/algo/daily/latest');

            if (!response.ok) {
                if (response.status === 404) {
                    this.showStatus('データがありません。スキャンを実行してください。', 'warning');
                    const area = document.getElementById('algo-content-area');
                    if (area) area.innerHTML = '<div class="card"><p>データがありません。スキャンを実行してください。</p></div>';
                    return;
                }
                throw new Error(`サーバーエラー: ${response.status}`);
            }

            this.summaryData = await response.json();
            this.currentView = 'summary';
            this.render();

            const { updated_at } = this.summaryData;
            const displayDate = this.formatDate(updated_at);

            this.showStatus(
                `最終更新: ${displayDate}`,
                'info'
            );

        } catch (error) {
            console.error('Algo data loading error:', error);
            this.showStatus(`❌ データ読み込みエラー: ${error.message}`, 'error');
        }
    }

    switchScreener(screenerKey) {
        document.querySelectorAll('.screener-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.screener === screenerKey);
        });

        this.activeScreener = screenerKey;
        this.render();
    }

    render() {
        if (!this.summaryData) return;

        // Render Portfolios
        if (this.currentView === 'summary' && this.summaryData.portfolios) {
            this.renderPortfolios(this.summaryData.portfolios);
        } else {
            const pContainer = document.getElementById('algo-portfolios');
            if (pContainer) pContainer.innerHTML = '';
        }

        const container = document.getElementById('algo-content-area');
        if (container) {
            container.innerHTML = '';
            this.renderSymbolList(container);
        } else {
            console.error('algo-content-area not found');
        }
    }

    renderPortfolios(portfolios) {
        const container = document.getElementById('algo-portfolios');
        if (!container) return;
        container.innerHTML = '';

        if (!portfolios || Object.keys(portfolios).length === 0) {
            return;
        }

        const title = document.createElement('h3');
        title.textContent = 'AI推奨モデルポートフォリオ';
        title.style.marginBottom = '15px';
        title.style.textAlign = 'center';
        container.appendChild(title);

        // Buttons
        const btnContainer = document.createElement('div');
        btnContainer.className = 'hwb-summary-grid';
        btnContainer.style.marginBottom = '20px';
        btnContainer.style.marginTop = '0';

        const types = [
            { key: 'aggressive', label: 'Aggressive', color: '#FF1744' },
            { key: 'balanced', label: 'Balanced', color: '#FFD600' },
            { key: 'defensive', label: 'Defensive', color: '#00C853' }
        ];

        types.forEach(type => {
            const btn = document.createElement('div');
            btn.className = `summary-card ${this.activePortfolio === type.key ? 'active' : ''}`;
            if (this.activePortfolio === type.key) {
                btn.style.border = `2px solid ${type.color}`;
                btn.style.backgroundColor = `${type.color}15`;
            } else {
                btn.style.border = '1px solid transparent';
            }

            btn.style.display = 'flex';
            btn.style.flexDirection = 'column';
            btn.style.alignItems = 'center';
            btn.style.justifyContent = 'center';
            btn.style.padding = '10px';

            const portfolioData = portfolios[type.key];
            const allocations = Array.isArray(portfolioData) ? portfolioData : (portfolioData?.allocations || []);
            const count = allocations.length;

            btn.innerHTML = `
                <h3 style="color: ${type.color}; margin-bottom: 5px;">${type.label}</h3>
                <p class="summary-count" style="font-size: 1.2em;">${count}銘柄</p>
            `;

            btn.addEventListener('click', () => {
                this.activePortfolio = type.key;
                this.renderPortfolios(portfolios);
            });

            btnContainer.appendChild(btn);
        });

        container.appendChild(btnContainer);

        // Content
        const activePortfolioData = portfolios[this.activePortfolio];
        const activeAllocations = Array.isArray(activePortfolioData) ? activePortfolioData : (activePortfolioData?.allocations || []);
        const activeCommentary = !Array.isArray(activePortfolioData) ? activePortfolioData?.commentary : null;

        if (activeAllocations && activeAllocations.length > 0) {
            const chartCard = document.createElement('div');
            chartCard.className = 'card';
            chartCard.style.padding = '15px';
            chartCard.style.marginTop = '10px';

            const canvasContainer = document.createElement('div');
            canvasContainer.style.position = 'relative';
            canvasContainer.style.height = '300px';
            canvasContainer.style.width = '100%';

            const canvas = document.createElement('canvas');
            canvasContainer.appendChild(canvas);
            chartCard.appendChild(canvasContainer);

            if (activeCommentary) {
                const commentaryDiv = document.createElement('div');
                commentaryDiv.style.marginTop = '20px';
                commentaryDiv.style.padding = '15px';
                commentaryDiv.style.backgroundColor = '#f9f9f9';
                commentaryDiv.style.borderRadius = '8px';
                commentaryDiv.style.borderLeft = '4px solid #006B6B';

                commentaryDiv.innerHTML = `
                    <h4 style="margin: 0 0 10px 0; color: #006B6B;">AI ポートフォリオ解説</h4>
                    <p style="margin: 0; line-height: 1.6; font-size: 0.95em;">${activeCommentary.replace(/\n/g, '<br>')}</p>
                `;
                chartCard.appendChild(commentaryDiv);
            }

            container.appendChild(chartCard);

            const labels = activeAllocations.map(d => d.ticker);
            const values = activeAllocations.map(d => d.percentage);
            const prices = activeAllocations.map(d => d.entry_price);
            const bgColors = this.generateColors(activeAllocations.length);

            new Chart(canvas, {
                type: 'pie',
                data: {
                    labels: labels,
                    datasets: [{
                        data: values,
                        backgroundColor: bgColors,
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: { boxWidth: 12, font: { size: 12 }, padding: 15 }
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    const label = context.label || '';
                                    const value = context.parsed || 0;
                                    const index = context.dataIndex;
                                    const price = prices[index];
                                    const priceText = price ? ` ($${price})` : '';
                                    return `${label}: ${value}%${priceText}`;
                                }
                            }
                        }
                    },
                    onClick: (event, elements) => {
                        if (elements.length > 0) {
                            const index = elements[0].index;
                            const ticker = labels[index];
                            this.setInputAndSearch(ticker);
                        }
                    }
                }
            });
        } else {
            container.innerHTML += `<p style="text-align:center; color:gray; padding:20px;">${this.activePortfolio}ポートフォリオのデータはありません。</p>`;
        }
    }

    generateColors(count) {
        const colors = [
            '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40',
            '#FF5252', '#448AFF', '#69F0AE', '#E040FB'
        ];
        return Array(count).fill().map((_, i) => colors[i % colors.length]);
    }

    setInputAndSearch(ticker) {
        const input = document.getElementById('algo-ticker-input');
        if (input) {
            input.value = ticker;
            this.searchTicker();
            const contentArea = document.getElementById('algo-content-area');
            if (contentArea) {
                contentArea.scrollIntoView({ behavior: 'smooth' });
            }
        }
    }

    renderSymbolList(container, data = null) {
        let screenerData;

        if (data) {
            screenerData = data;
        } else {
            const { summary } = this.summaryData;
            screenerData = summary[this.activeScreener] || [];
        }

        if (screenerData.length === 0) {
            container.innerHTML += '<p class="no-data">このスクリーナーには該当銘柄がありません。</p>';
            return;
        }

        const sortOrder = { 'contraction': 0, 'transition': 1, 'expansion': 2 };
        const sortedData = [...screenerData].sort((a, b) => {
            return sortOrder[a.volatility_regime] - sortOrder[b.volatility_regime];
        });

        const listDiv = document.createElement('div');
        listDiv.className = 'algo-symbol-list-one-col';

        sortedData.forEach(item => {
            const symbolItem = this.createSymbolItem(item);
            listDiv.appendChild(symbolItem);
        });

        container.appendChild(listDiv);
    }

    createSymbolItem(item) {
        const itemDiv = document.createElement('div');
        itemDiv.className = 'algo-symbol-item-card';

        const chartUrl = `/charts/algo/${item.symbol}_gamma_analysis.png?v=${new Date().getTime()}`;
        const regimeColor = this.getRegimeColor(item.volatility_regime);

        const aiAnalysis = item.gemini_analysis ?
            (typeof item.gemini_analysis === 'string' ? item.gemini_analysis : JSON.stringify(item.gemini_analysis))
            : 'AI解説なし';

        const sectorInfo = (item.sector && item.sector !== 'Unknown') ? item.sector : '-';
        const industryInfo = (item.industry && item.industry !== 'Unknown') ? item.industry : '-';

        itemDiv.innerHTML = `
            <div class="algo-card-header">
                 <div class="algo-ticker-wrapper">
                     <span class="algo-card-ticker">${item.symbol}</span>
                     <span class="algo-info-text">${sectorInfo} / ${industryInfo}</span>
                 </div>
                 <span class="status-dot" style="background-color: ${regimeColor};"></span>
            </div>
            <div class="algo-card-image">
                <img src="${chartUrl}" alt="${item.symbol} Analysis" class="algo-chart-img" loading="lazy" onerror="this.onerror=null;this.src='data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAzMDAgMjAwIj48cmVjdCB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiBmaWxsPSIjZjBmMGYwIi8+PHRleHQgeD0iNTAlIiB5PSI1MCUiIGRvbWluYW50LWJhc2VsaW5lPSJtaWRkbGUiIHRleHQtYW5jaG9yPSJtaWRkbGUiIGZpbGw9IiM5OTkiPk5vIENoYXJ0PC90ZXh0Pjwvc3ZnPg==';">
            </div>
            <div class="algo-card-footer">
                <p class="algo-ai-text">${aiAnalysis.replace(/\n/g, '<br>')}</p>
            </div>
        `;

        const img = itemDiv.querySelector('.algo-chart-img');
        if (img) {
            this.addDoubleTapListener(img, item.symbol);
        }

        return itemDiv;
    }

    renderMetaInfo(item) {
        return '';
    }

    getScreenerDisplayName(screenerKey) {
        return screenerKey;
    }

    getRegimeColor(regime) {
        const colors = {
            'contraction': '#00C853',
            'transition': '#FFD600',
            'expansion': '#FF1744'
        };
        return colors[regime] || '#888888';
    }

    getRegimeLabel(regime) {
        const labels = {
            'contraction': '🟢 凪',
            'transition': '🟡 通常',
            'expansion': '🔴 嵐'
        };
        return labels[regime] || regime;
    }

    getRSClass(rs) {
        if (rs >= 90) return 'rs-excellent';
        if (rs >= 80) return 'rs-good';
        return 'rs-average';
    }

    formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            const date = new Date(dateStr);
            return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
        } catch (e) { return dateStr; }
    }

    addDoubleTapListener(element, symbol) {
        let lastTap = 0;
        element.addEventListener('touchend', (e) => {
            const currentTime = new Date().getTime();
            const tapLength = currentTime - lastTap;
            if (tapLength < 500 && tapLength > 0) {
                e.preventDefault();
                this.showImagePopup(symbol);
            }
            lastTap = currentTime;
        });
        element.addEventListener('dblclick', () => {
            this.showImagePopup(symbol);
        });
    }

    async showImagePopup(symbol) {
        // Implementation omitted for brevity, reusing generic one if needed or just alert
        const zoomChartUrl = `/charts/algo/${symbol}_gamma_analysis_3m.png?v=${new Date().getTime()}`;
        const overlay = document.createElement('div');
        overlay.className = 'image-popup-overlay algo-popup-overlay';
        const contentDiv = document.createElement('div');
        contentDiv.className = 'algo-popup-content';
        contentDiv.style.backgroundColor = 'white';
        contentDiv.style.padding = '10px';
        contentDiv.style.borderRadius = '8px';
        contentDiv.style.maxWidth = '95%';
        contentDiv.style.maxHeight = '90vh';
        contentDiv.style.overflowY = 'auto';
        const img = document.createElement('img');
        img.src = zoomChartUrl;
        img.className = 'algo-popup-image';
        img.style.width = '100%';
        img.style.height = 'auto';
        img.onerror = function() {
            this.src = `/charts/algo/${symbol}_gamma_analysis.png?v=${new Date().getTime()}`;
        };
        contentDiv.appendChild(img);
        overlay.appendChild(contentDiv);
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) document.body.removeChild(overlay);
        });
        document.body.appendChild(overlay);
    }

    async searchTicker() {
        const input = document.getElementById('algo-ticker-input');
        const ticker = input.value.trim().toUpperCase();
        if (!ticker) {
            this.showStatus('ティッカーシンボルを入力してください', 'warning');
            return;
        }
        this.showStatus(`${ticker}のデータを検索中...`, 'info');
        if (this.summaryData && this.summaryData.summary) {
            const allItems = [];
            const seenSymbols = new Set();
            for (const screenerKey in this.summaryData.summary) {
                const items = this.summaryData.summary[screenerKey];
                for (const item of items) {
                    if (item.symbol === ticker && !seenSymbols.has(item.symbol)) {
                        allItems.push(item);
                        seenSymbols.add(item.symbol);
                    }
                }
            }
            if (allItems.length > 0) {
                const container = document.getElementById('algo-content-area');
                if (container) {
                    container.innerHTML = '';
                    this.renderSymbolList(container, allItems);
                }
                const searchBtn = document.getElementById('algo-analyze-btn');
                if (searchBtn) {
                    searchBtn.textContent = 'リセット';
                    searchBtn.dataset.state = 'reset';
                }
                this.showStatus(`✅ ${ticker}が当日のスクリーニングで見つかりました`, 'info');
                return;
            }
        }
        try {
            const response = await fetchWithAuth(`/api/algo/analyze_ticker?ticker=${ticker}&force=false`);
            if (!response.ok) {
                if (response.status === 404) {
                    const shouldAnalyze = confirm(`${ticker}は本日のスクリーナーに含まれていません。\n新規に分析を実行しますか？（30秒ほどかかります）`);
                    if (shouldAnalyze) {
                        await this.forceAnalyzeTicker(ticker);
                    } else {
                        this.showStatus('検索をキャンセルしました', 'info');
                    }
                    return;
                }
                throw new Error(`検索に失敗しました: ${response.status}`);
            }
            const symbolData = await response.json();
            this.renderSearchResults(ticker, symbolData);
            const searchBtn = document.getElementById('algo-analyze-btn');
            if (searchBtn) {
                searchBtn.textContent = 'リセット';
                searchBtn.dataset.state = 'reset';
            }
            this.showStatus(`✅ ${ticker}の検索結果を表示中`, 'info');
        } catch (error) {
            console.error('Search error:', error);
            this.showStatus(`❌ エラー: ${error.message}`, 'error');
        }
    }

    async forceAnalyzeTicker(ticker) {
        this.showStatus(`${ticker}を分析中...`, 'info');
        try {
            const response = await fetchWithAuth(`/api/algo/analyze_ticker?ticker=${ticker}&force=true`);
            if (!response.ok) throw new Error(`分析失敗: ${response.status}`);
            const symbolData = await response.json();
            this.renderSearchResults(ticker, symbolData);
            const searchBtn = document.getElementById('algo-analyze-btn');
            if (searchBtn) {
                searchBtn.textContent = 'リセット';
                searchBtn.dataset.state = 'reset';
            }
            this.showStatus(`✅ ${ticker}の分析完了`, 'info');
        } catch (e) {
            this.showStatus(`❌ 分析エラー: ${e.message}`, 'error');
        }
    }

    renderSearchResults(ticker, symbolData) {
        const container = document.getElementById('algo-content-area');
        if (!container) return;
        container.innerHTML = '';
        const resultDiv = document.createElement('div');
        resultDiv.className = 'algo-search-results';
        resultDiv.innerHTML = `
            <h2>${ticker} 分析結果</h2>
            <div class="hwb-symbol-item algo-symbol-item" style="flex-direction:column; align-items:flex-start;">
                <div class="hwb-symbol-header" style="width:100%; display:flex; justify-content:space-between;">
                    <span class="hwb-symbol-name">${ticker}</span>
                    <span class="regime-badge regime-${symbolData.volatility_regime}">${this.getRegimeLabel(symbolData.volatility_regime)}</span>
                </div>
                <div style="width:100%; margin-top:15px;">
                    <img src="${symbolData.analysis_data.gamma_plot}" style="width:100%; height:auto; border-radius:4px;">
                </div>
                <div style="margin-top:20px; background:#f9f9f9; padding:15px; border-radius:8px; width:100%;">
                    ${symbolData.gemini_analysis ?
                        `<h3>AI解説</h3><p>${symbolData.gemini_analysis.replace(/\n/g, '<br>')}</p>` :
                        `<p class="info-message">${symbolData.message || '解説データなし'}</p>`
                    }
                </div>
            </div>
        `;
        container.appendChild(resultDiv);
    }

    resetToSummary() {
        this.currentView = 'summary';
        const input = document.getElementById('algo-ticker-input');
        if (input) input.value = '';
        const searchBtn = document.getElementById('algo-analyze-btn');
        if (searchBtn) {
            searchBtn.textContent = '検索';
            searchBtn.dataset.state = 'search';
        }
        this.render();
    }

    showStatus(message, type = 'info') {
        const statusDiv = document.getElementById('algo-status');
        if (statusDiv) {
            statusDiv.textContent = message;
            statusDiv.className = `algo-status-info ${type}`;
        }
    }
}


function initAlgoTab() {
    window.algoManager = new AlgoManager();
    console.log('AlgoManager initialized');
}

// Hook into initialization
if (document.getElementById('algo-content')) {
    // We can init immediately if the tab exists
    initAlgoTab();
}

// Also hook into tab switching to load data on first view
document.querySelector('.tab-container').addEventListener('click', (e) => {
    if (e.target.dataset.tab === 'algo' && window.algoManager) {
        if (!window.algoManager.summaryData) {
            window.algoManager.loadData();
        }
    }
});
