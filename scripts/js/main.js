// Main client script for Pyodide + DuckDB backtest SPA (moved to scripts/js)
const spinner = document.getElementById('spinner');
const status = document.getElementById('status');
const runBtn = document.getElementById('runBtn');
const dataOut = document.getElementById('dataOut');
const resultOut = document.getElementById('resultOut');
const errorOut = document.getElementById('errorOut');
const pathBanner = document.getElementById('pathBanner');

function showSpinner(msg){ spinner.style.display='flex'; status.textContent = msg; }
function hideSpinner(){ spinner.style.display='none'; }

let pyodide = null;
let duckWorker = null;
let duckdbInstance = null;
let duckdbConn = null;
let duckdbWorkerUrl = null;
let arrowLib = null; // 'pyarrow' | 'arro3' | null
let APP_CONFIG = null;
// Chart state
let tvChart = null;
let candleSeries = null;

function displayDebug(obj){
  try{
    if(!obj) return;
    if(obj.debug){
      console.log('backtest debug:', obj.debug);
      try{ if(pathBanner){ pathBanner.style.display='block'; pathBanner.textContent += ` | debug: date_col=${obj.debug.date_col}`; } }catch(e){}
      try{ if(resultOut){ resultOut.textContent += '\ndebug: ' + JSON.stringify(obj.debug); } }catch(e){}
    }
  }catch(e){ console.warn('displayDebug failed', e); }
}

function renderCandles(records){
  if(!records || records.length === 0){
    const container = document.getElementById('chart');
    if(container) container.innerHTML = '<div style="padding:12px;color:#666">No chartable data</div>';
    return;
  }

  const LC = window.LightweightCharts;
  if(!LC) throw new Error('LightweightCharts not loaded — check network access to cdn.jsdelivr.net');

  // normalise each record's time to Unix seconds (LightweightCharts requirement)
  const normalizeTime = (t) => {
    if(t == null) return null;
    const n = Number(t);
    if(Number.isFinite(n)){
      // Handle common magnitudes: ns (~1e18), ms (~1e12), s (~1e9)
      if(n > 1e14) return Math.floor(n / 1e9); // ns -> s
      if(n > 1e12) return Math.floor(n / 1000); // ms -> s
      if(n > 1e9)  return Math.floor(n);         // already seconds
      // Small numeric values: try plausible scalings to map to a reasonable year
      const candidates = [1e6, 1e3, 1];
      for(const m of candidates){
        const ms = n * m;
        const yr = new Date(ms).getUTCFullYear();
        if(yr >= 1990 && yr <= 2050){
          return Math.floor(ms / 1000);
        }
      }
      return null;
    }
    const p = Date.parse(String(t));
    return isNaN(p) ? null : Math.floor(p / 1000);
  };

  console.log('renderCandles sample record:', records[0], 'total:', records.length);

  const data = records
    .map(r => ({
      time:  normalizeTime(r.time || r.timestamp || r.datetime),
      open:  Number(r.open),
      high:  Number(r.high),
      low:   Number(r.low),
      close: Number(r.close)
    }))
    .filter(d => d.time && Number.isFinite(d.open) && Number.isFinite(d.high) && Number.isFinite(d.low) && Number.isFinite(d.close));
  console.log('renderCandles filtered data length:', data.length, 'sample:', data[0]);

  if(!tvChart){
    const container = document.getElementById('chart');
    container.innerHTML = '';
    tvChart = LC.createChart(container, {
      width: Math.max(300, container.getBoundingClientRect().width),
      height: 400,
      layout: { background: { color: '#ffffff' }, textColor: '#333' },
      grid:   { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } }
    });
    candleSeries = tvChart.addCandlestickSeries();
    window.addEventListener('resize', () => {
      const rect = container.getBoundingClientRect();
      tvChart.resize(Math.max(300, rect.width), 400);
    });
  }

  if(data.length === 0){
    const container = document.getElementById('chart');
    if(container) container.innerHTML = '<div style="padding:12px;color:#666">No OHLC data detected for chart</div>';
    return;
  }

  candleSeries.setData(data);
}


const DEFAULT_CONFIG = {
  pyodideIndex: 'https://cdn.jsdelivr.net/pyodide/v0.27.0/full/',
  GITHUB_USER: 'your-github-user',
  GITHUB_REPO: 'wasm-quant',
  GITHUB_BRANCH: 'main',
  encryptUrls: false
};

async function loadConfig(){
  if(APP_CONFIG) return APP_CONFIG;
  try{
    const resp = await fetch('/config.json');
    if(resp.ok){
      APP_CONFIG = Object.assign({}, DEFAULT_CONFIG, await resp.json());
    }else{
      APP_CONFIG = DEFAULT_CONFIG;
    }
  }catch(e){ APP_CONFIG = DEFAULT_CONFIG; }
  return APP_CONFIG;
}

// Normalize rows returned from duckdb-wasm to an array of plain objects.
function normalizeRows(rows){
  if(!rows || !rows.length) return [];
  const rep = rows[0];
  if(!rep) return [];
  // Arrow StructRow (Proxy(Me)) — use toJSON() to get named plain objects
  if(typeof rep.toJSON === 'function'){
    return rows.map(r => r ? r.toJSON() : {});
  }
  // Already plain objects with keys
  if(typeof rep === 'object' && !Array.isArray(rep) && Object.keys(rep).length > 0){
    return rows.map(r => r || {});
  }
  // Positional array rows — map to col0..colN
  if(Array.isArray(rep)){
    const n = rep.length;
    return rows.map(r=>{
      if(r==null) return {};
      const obj = {};
      for(let i=0;i<n;i++) obj[`col${i}`] = r[i];
      return obj;
    });
  }
  // Last resort: numeric index probe for Proxy rows without toJSON
  if(typeof rep === 'object'){
    const maxProbe = 64;
    const found = [];
    for(let i=0;i<maxProbe;i++){
      try{ const v=rep[i]; if(v===undefined){ if(found.length) break; } else found.push(i); }catch(e){ break; }
    }
    if(found.length){
      return rows.map(r=>{
        if(!r) return {};
        const obj={};
        for(let i=0;i<found.length;i++) obj[`col${i}`]=r[i];
        return obj;
      });
    }
  }
  return rows.map(r=> r!=null ? {value: r} : {});
}

// esm.sh rewrites all bare specifiers (apache-arrow, qs, etc.) server-side —
// the browser receives a single resolved module with no bare imports.
const DUCKDB_ESM = 'https://esm.sh/@duckdb/duckdb-wasm';
const ARROW_ESM = 'https://esm.sh/apache-arrow@10.0.0';

async function initDuckDBWasm(){
  if(duckdbInstance && duckdbConn) return;
  const duck = await import(DUCKDB_ESM);
  // Workers must be same-origin. Fetch the worker script and create a blob URL
  // to avoid the cross-origin Worker restriction (works with any CDN).
  const bundle = await duck.selectBundle(duck.getJsDelivrBundles());
  const workerResp = await fetch(bundle.mainWorker);
  const workerText = await workerResp.text();
  const workerBlob = new Blob([workerText], { type: 'text/javascript' });
  const workerUrl = URL.createObjectURL(workerBlob);
  const worker = new Worker(workerUrl);
  const logger = new duck.ConsoleLogger();
  duckdbInstance = new duck.AsyncDuckDB(logger, worker);
  await duckdbInstance.instantiate(bundle.mainModule, bundle.pthreadWorker ?? null);
  duckdbConn = await duckdbInstance.connect();
}

// Query using the AsyncDuckDB instance (worker-backed) and return Arrow IPC Uint8Array.
// The URL is embedded directly in the SQL — no registerFileURL needed.
// If `asArrow` is true, return an object {type:'arrow', buffer: Uint8Array}
// otherwise return an array of JS row objects.
async function queryParquetWithWorker(url, start, end, asArrow=false){
  await initDuckDBWasm();
  // Sanitise inputs minimally (we'll not rely on schema here)
  const safeUrl = String(url).replace(/'/g, "");
  const safeStart = start ? String(start).replace(/[^0-9\-]/g, '') : null;
  const safeEnd = end ? String(end).replace(/[^0-9\-]/g, '') : null;
  // Build SQL without hardcoding column names; select * and limit rows for preview
  let sql = `SELECT * FROM read_parquet('${safeUrl}')`;
  if(safeStart && safeEnd){
    // We don't know the date column name, so only apply limits — avoid WHERE
  }
  sql += ' LIMIT 100';

  const res = await duckdbConn.query(sql);
  if(asArrow){
    // Helper: try multiple serialization methods for an Arrow Table-like object
    async function trySerializeArrowTable(table){
      if(!table) return null;
      try{
        if(typeof table.serialize === 'function'){
          const v = table.serialize();
          if(v instanceof Uint8Array) return v;
          if(v && v.buffer) return new Uint8Array(v.buffer);
        }
      }catch(e){ console.warn('table.serialize failed:', e); }
      try{
        if(typeof table.toUint8Array === 'function'){
          return table.toUint8Array();
        }
      }catch(e){ console.warn('table.toUint8Array failed:', e); }
      try{
        if(typeof table.toArrayBuffer === 'function'){
          const b = table.toArrayBuffer();
          return new Uint8Array(b);
        }
      }catch(e){ console.warn('table.toArrayBuffer failed:', e); }
      try{
        if(typeof table.toBuffer === 'function'){
          const b = table.toBuffer();
          return new Uint8Array(b);
        }
      }catch(e){ console.warn('table.toBuffer failed:', e); }
      // Try apache-arrow RecordBatchWriter as last resort
      try{
        const arrow = await import(ARROW_ESM);
        if(arrow && arrow.RecordBatchWriter && typeof arrow.RecordBatchWriter.writeAll === 'function'){
          try{
            const writer = arrow.RecordBatchWriter.writeAll(table);
            if(writer){
              if(typeof writer.toUint8Array === 'function') return writer.toUint8Array();
              if(typeof writer.toArrayBuffer === 'function') return new Uint8Array(writer.toArrayBuffer());
            }
          }catch(e){ console.warn('RecordBatchWriter.writeAll failed:', e); }
        }
      }catch(e){ console.warn('Importing apache-arrow for writer fallback failed:', e); }
      return null;
    }

    // Debug: log what the result exposes
    try{
      console.debug('queryParquetWithWorker: result shape', {
        hasToArrow: res && typeof res.toArrow === 'function',
        hasSchema: !!(res && res.schema),
        numRows: res && res.numRows,
        hasToArray: res && typeof res.toArray === 'function'
      });
    }catch(e){ console.debug('queryParquetWithWorker: debug probe failed', e); }

    // 1) Try duckdb-wasm native toArrow -> serialize
    if(res && typeof res.toArrow === 'function'){
      try{
        const arrowTable = await res.toArrow();
        console.debug('arrowTable created, probing serialization methods on table');
        if(arrowTable){
          try{
            console.debug('arrowTable keys:', Object.keys(arrowTable));
          }catch(e){}
        }
        const uint8 = await trySerializeArrowTable(arrowTable);
        if(uint8) return { type: 'arrow', buffer: uint8 };
      }catch(e){ console.warn('res.toArrow() or serialization failed:', e); }
    }

    // 2) If duckdb didn't serialize, try to build a JS apache-arrow Table from columns
    try{
      const arrow = await import(ARROW_ESM);
      try{ console.debug('imported apache-arrow module keys:', Object.keys(arrow).slice(0,40)); }catch(e){}
      if(!res || !res.schema || !res.schema.fields) throw new Error('No schema to build Arrow');
      const colNames = res.schema.fields.map(f => f.name);
      const numRows = res.numRows || 0;
      const cols = {};
      for(const name of colNames){
        const col = res.getChild(name);
        const arr = new Array(numRows);
        for(let i=0;i<numRows;i++){
          try{ arr[i] = col ? col.get(i) : null; }catch(e){ arr[i] = null; }
        }
        cols[name] = arr;
      }
      let table = null;
      if(typeof arrow.tableFromArrays === 'function'){
        table = arrow.tableFromArrays(cols);
      }else if(typeof arrow.Table === 'function'){
        try{ table = arrow.Table.new(cols); }catch(e){ console.warn('arrow.Table.new failed:', e); }
      }
      if(table){
        try{ console.debug('constructed JS arrow table, probing methods'); }catch(e){}
        const uint8 = await trySerializeArrowTable(table);
        if(uint8) return { type: 'arrow', buffer: uint8 };
      }
    }catch(e){ console.warn('Building Arrow IPC in JS failed:', e); }

      // As a fallback when serialization fails, return column-wise JS arrays or
      // typed ArrayBuffers so the Python side can reconstruct a table without
      // relying on IPC bytes.
    try{
      if(res && res.schema && res.schema.fields){
        const colNames = res.schema.fields.map(f => f.name);
        const numRows = res.numRows || 0;
        const cols = {};
        const dtypes = {};
        for(const name of colNames){
          const col = res.getChild(name);
          // Probe first 64 non-null values to determine type
          const probe = [];
          for(let i=0;i<Math.min(64, numRows); i++){
            try{ const v = col ? col.get(i) : null; if(v !== null && v !== undefined) probe.push(v); }catch(e){}
          }
          let kind = 'object';
          if(probe.length > 0){
            const allNumbers = probe.every(v => typeof v === 'number');
            const allInts = allNumbers && probe.every(v => Number.isInteger(v));
            const allBools = probe.every(v => typeof v === 'boolean');
            const allDates = probe.every(v => (v instanceof Date) || (typeof v === 'string' && !isNaN(Date.parse(v))));
            if(allBools) kind = 'bool';
            else if(allInts) kind = 'int';
            else if(allNumbers) kind = 'float';
            else if(allDates) kind = 'datetime';
            else kind = 'object';
          }
          // Build typed buffer for numeric/bool columns
          if(kind === 'int' || kind === 'float' || kind === 'bool'){
            const arr = new Float64Array(numRows);
            for(let i=0;i<numRows;i++){
              try{ const v = col ? col.get(i) : null; arr[i] = (v==null) ? NaN : Number(v); }catch(e){ arr[i] = NaN; }
            }
            // store as Uint8Array view for transfer
            const bytes = new Uint8Array(arr.buffer);
            cols[name] = bytes;
            // Store numeric columns as float64 bytes for safe round-trip to Python
            dtypes[name] = (kind === 'bool') ? 'bool' : 'float64';
          }else{
            // fallback to JS array for objects/strings/datetimes
            const arr = new Array(numRows);
            for(let i=0;i<numRows;i++){
              try{ arr[i] = col ? col.get(i) : null; }catch(e){ arr[i] = null; }
            }
            cols[name] = arr;
            dtypes[name] = 'object';
          }
        }
        console.debug('queryParquetWithWorker: returning column-wise fallback', Object.keys(cols));
        return { type: 'cols_typed', columns: cols, dtypes: dtypes, nrows: numRows };
      }
    }catch(e){ console.warn('Column-wise fallback failed:', e); }

    throw new Error('Arrow serialization not supported by result');
  }
  // duckdbConn.query() returns an Apache Arrow Table directly.
  // Extract data column-by-column using the schema to get real column names
  // and avoid the Proxy(Me) / StructRow issue from toArray().
  if(res && res.schema && res.schema.fields && res.schema.fields.length > 0){
    const colNames = res.schema.fields.map(f => f.name);
    const numRows = res.numRows;
    const result = [];
    for(let i = 0; i < numRows; i++){
      const row = {};
      for(const name of colNames){
        const col = res.getChild(name);
        row[name] = col ? col.get(i) : null;
      }
      result.push(row);
    }
    console.log('duckdb schema cols:', colNames, 'sample:', result.slice(0,2));
    return result;
  }
  // Fallback: toArray() returns Arrow StructRow Proxy objects — extract via toJSON()
  if(res && typeof res.toArray === 'function'){
    const maybeRows = res.toArray();
    const first = maybeRows[0];
    if(first && typeof first.toJSON === 'function'){
      return maybeRows.map(r => r.toJSON());
    }
    return maybeRows;
  }
  throw new Error('Unable to convert query result to JS rows');
}

async function initDuckBack(){
  showSpinner('Initializing Pyodide...');
  // Ensure configuration is loaded before deciding which Pyodide index to use
  await loadConfig();
  // loadPyodide was provided by the script in index.html (v0.27+)
  // If the user provided a custom Pyodide build URL, try loading from it first.
  // const pyodideUrlInput = document.getElementById('pyodideUrl');
  // const customIndexURL = pyodideUrlInput && pyodideUrlInput.value.trim() ? pyodideUrlInput.value.trim() : null;

  // Helper: inject a remote script tag and wait for load with timeout
  const injectScript = (url, ms = 15000) => new Promise((resolve, reject) => {
    // remove any previously injected pyodide script
    const prev = document.querySelector('script[data-pyodide-injected]');
    if(prev) prev.remove();
    const s = document.createElement('script');
    s.src = url;
    s.async = true;
    s.setAttribute('data-pyodide-injected', '1');
    const t = setTimeout(()=>{
      s.onload = s.onerror = null;
      reject(new Error('Script load timeout'));
    }, ms);
    s.onload = () => { clearTimeout(t); resolve(s); };
    s.onerror = (e) => { clearTimeout(t); reject(e || new Error('Script load error')); };
    document.head.appendChild(s);
  });

  // Try loading arrow-supporting packages via pyodide.loadPackage() on an already-loaded pyodide instance
  const tryLoadArrowLibs = async (py) => {
    try{
      await py.loadPackage(["pyarrow", "pandas"]);
      return 'pyarrow';
    }catch(e){
      try{
        await py.loadPackage(["arro3-core", "arro3-io", "pandas"]);
        return 'arro3';
      }catch(e2){
        return null;
      }
    }
  };

  // If user specified a custom index input, keep it as override, but prefer APP_CONFIG
  // Use configured pyodide index from config.json; if not present, fall back to custom input or default
  const pinnedIndex = (APP_CONFIG && APP_CONFIG.pyodideIndex);
  try{
    showSpinner('Loading Pyodide from ' + pinnedIndex);
    const scriptUrl = pinnedIndex.endsWith('/') ? pinnedIndex + 'pyodide.js' : (pinnedIndex + '/pyodide.js');
    await injectScript(scriptUrl, 20000);
    pyodide = await loadPyodide({ indexURL: pinnedIndex });
    const pyVer = pyodide ? (pyodide.version || 'unknown') : 'unknown';
    console.log('Loaded Pyodide runtime version:', pyVer, 'from', pinnedIndex);
    // Try pyarrow first, then arro3
    arrowLib = await tryLoadArrowLibs(pyodide);
    console.log('Detected arrow library:', arrowLib);
  }catch(e){
    console.warn('Failed loading pinned Pyodide or arrow libs:', e);
    throw e;
  }

  // Ensure numpy and pandas are available for column reconstruction
  try{
    await pyodide.loadPackage(["numpy", "pandas"]);
  }catch(e){
    console.warn('Failed to load numpy/pandas packages:', e);
  }

  if(!arrowLib){
    console.warn('No Arrow backend available on pinned Pyodide; will use JS rows fallback.');
  }

  showSpinner('Pandas ready — loading Python helpers...');
  // Load the bridge and postprocess modules into Pyodide
  const respBridge = await fetch('scripts/py/bridge.py');
  const respPost = await fetch('scripts/py/postprocess.py');
  if(!respBridge.ok) throw new Error('Failed to load scripts/py/bridge.py: ' + respBridge.statusText);
  if(!respPost.ok) throw new Error('Failed to load scripts/py/postprocess.py: ' + respPost.statusText);
  const [bridgeCode, postCode] = await Promise.all([respBridge.text(), respPost.text()]);
  try{
    await pyodide.runPythonAsync(bridgeCode);
    await pyodide.runPythonAsync(postCode);
    // Ensure a `postprocess` namespace exists in globals for JS to reference
    try{
      await pyodide.runPythonAsync("import types\npostprocess = types.SimpleNamespace(chartify_records=chartify_records)");
    }catch(e){ console.warn('Could not create postprocess namespace:', e); }
  }catch(e){
    console.error('Error executing bridge/postprocess Python modules:', e);
    throw e;
  }

  // Provide the page origin to Python so helpers can build absolute URLs to local files
  try{
    await pyodide.runPythonAsync(`ORIGIN = "${location.origin}"`);
  }catch(e){
    console.warn('Could not set ORIGIN in Python globals:', e);
  }

  // helper to query via duckdb-wasm worker
  hideSpinner();
  runBtn.disabled = false;
  status.textContent = 'Ready';
}

// Note: Pyodide 0.27+ includes a prebuilt `pyarrow`, so we load it via `loadPackage`.

// Start initialization using the DuckDB-ready flow
initDuckBack().catch(err=>{
  hideSpinner(); runBtn.disabled = true;
  errorOut.classList.remove('hidden');
  errorOut.textContent = 'Failed initializing Pyodide or packages: ' + err;
});

// remove stray initializer call (we use initDuckBack)

async function run(){
  errorOut.classList.add('hidden'); if(dataOut) dataOut.textContent = '—'; if(resultOut) resultOut.textContent = '—';
  const ticker = document.getElementById('ticker').value.trim();
  const start = document.getElementById('start').value;
  const end = document.getElementById('end').value;
  if(!ticker){ errorOut.classList.remove('hidden'); errorOut.textContent='Enter a ticker.'; return; }

  runBtn.disabled = true; showSpinner('Running backtest (DuckDB reading remote Parquet)...');
  try{
    const pyRun = pyodide.globals.get('run_backtest');
    const res = await pyRun.call(ticker, start, end);
    const obj = res.toJs ? res.toJs() : res;
    if(obj && obj.error){
      const msg = String(obj.error);
      let hint = '';
      if(/CORS|Access-Control|Cross-origin|No 'Access-Control'/.test(msg) || /Failed to fetch/.test(msg)){
        hint = "\nCORS likely blocked the request. Try serving the Parquet via a CDN/proxy such as jsDelivr or enable CORS on the host. Example: https://cdn.jsdelivr.net/gh/<user>/<repo>/data/" + ticker + "_1min.parquet";
      }
      errorOut.classList.remove('hidden');
      errorOut.textContent = 'Error fetching/parsing Parquet: ' + msg + hint;
      if(dataOut) dataOut.textContent = '—'; if(resultOut) resultOut.textContent = '—';
    }else{
      if(dataOut) dataOut.textContent = JSON.stringify(obj.head, null, 2);
      const src = obj.source ? ` (source: ${obj.source})` : '';
      if(resultOut) resultOut.textContent = 'Total return: ' + (obj.total_return !== undefined ? obj.total_return.toFixed(2) + '%' : 'N/A') + src;
      if(obj && obj.records){ try{ renderCandles(obj.records); }catch(chartErr){ errorOut.classList.remove('hidden'); errorOut.textContent = 'Chart error: ' + chartErr; } }
    }
    try{ res.destroy && res.destroy(); }catch(e){}
  }catch(err){
    let text = String(err);
    let hint = '';
    if(/CORS|Failed to fetch|NetworkError/.test(text)){
      hint = "\nIf using GitHub raw URLs, raw.githubusercontent.com may block CORS. Try a CDN proxy like jsDelivr (https://www.jsdelivr.com/) or host the files with CORS enabled.";
    }
    errorOut.classList.remove('hidden'); errorOut.textContent = text + hint;
  }finally{
    hideSpinner(); runBtn.disabled = false;
  }
}

// Wire the UI button to the worker-based path to avoid invoking DuckDB httpfs inside Pyodide
runBtn.addEventListener('click', runWithWorker);

// Expose a button path that prefers duckdb-wasm worker for remote parquet
async function runWithWorker(){
  const ticker = document.getElementById('ticker').value.trim();
  const start = document.getElementById('start').value;
  const end = document.getElementById('end').value;
  const useLocal = document.getElementById('useLocal').checked;
  // Choose URL: local test file or remote jsDelivr-proxied Parquet (CORS-friendly).
  // jsDelivr serves GitHub-hosted files with Access-Control-Allow-Origin: *
  // Pattern: https://cdn.jsdelivr.net/gh/<user>/<repo>@<branch>/data/<file>.parquet
  const GITHUB_USER = (APP_CONFIG && APP_CONFIG.GITHUB_USER);
  const GITHUB_REPO = (APP_CONFIG && APP_CONFIG.GITHUB_REPO);
  const GITHUB_BRANCH = (APP_CONFIG && APP_CONFIG.GITHUB_BRANCH);
  // const localFile = `data/SPX_1d.parquet`;
  const localFile = `data/SPX_4h.parquet`;
  const remoteUrl = useLocal
    ? `${location.origin}/${localFile}`
    : `https://cdn.jsdelivr.net/gh/${GITHUB_USER}/${GITHUB_REPO}@${GITHUB_BRANCH}/SPX_1d.parquet`;
  showSpinner('Querying Parquet via duckdb-wasm worker...');
  console.log('Running with worker, URL:', remoteUrl, 'start:', start, 'end:', end);
  try{
    // Ensure Python helpers are the latest version (reload in Pyodide at runtime)
    try{
      const [respBridge, respPost] = await Promise.all([
        fetch('scripts/py/bridge.py'),
        fetch('scripts/py/postprocess.py')
      ]);
      if(respBridge && respBridge.ok){ const bridgeCodeLatest = await respBridge.text(); try{ await pyodide.runPythonAsync(bridgeCodeLatest); console.debug('Reloaded Python bridge.py'); }catch(e){ console.warn('Reloading bridge.py failed:', e); } }
      if(respPost && respPost.ok){
        try{
          const postCodeLatest = await respPost.text();
          await pyodide.runPythonAsync(postCodeLatest);
          // re-create postprocess namespace
          try{ await pyodide.runPythonAsync("import types\npostprocess = types.SimpleNamespace(chartify_records=chartify_records)"); }catch(e){ console.warn('Could not re-create postprocess namespace after reload:', e); }
          console.debug('Reloaded Python postprocess.py');
        }catch(e){ console.warn('Reloading postprocess.py failed:', e); }
      }
    }catch(e){ console.warn('Could not reload Python modules before running backtest:', e); }
    // Use the detection result from init; prefer Arrow zero-copy path only when pyarrow is present.
    // If arro3 is present in Pyodide, use the JS-rows path and let Pyodide's arro3/pandas handle it.
    if(arrowLib === 'pyarrow'){
      if(pathBanner){ pathBanner.style.display='block'; pathBanner.textContent = `Data path: Arrow (pyarrow available)`; }
      try{
        const res = await queryParquetWithWorker(remoteUrl, start, end, true);
        if(!res) throw new Error('Did not receive Arrow payload');
        if(res.type === 'arrow'){
          const uint8 = res.buffer;
          const pyBuf = pyodide.unpack_buffer(uint8);
          let pres = null;
          let cleaned = null;
          let pyBridge = null;
          let pyPost = null;
          try{
            pyBridge = pyodide.globals.get('bridge');
            pyPost = pyodide.globals.get('postprocess');
            pres = pyBridge.from_arrow(pyBuf);
            cleaned = pyPost.chartify_records(pres);
            const pobj = cleaned.toJs ? cleaned.toJs() : cleaned;
            try{ displayDebug(pobj); }catch(e){}
              if(pobj && pobj.error){
                  errorOut.classList.remove('hidden'); errorOut.textContent = pobj.error;
                }else{
                  if(dataOut) dataOut.textContent = JSON.stringify(pobj.head || [], null, 2);
                  if(resultOut) resultOut.textContent = `Rows: ${pobj.nrows || 'N/A'} | Columns: ${(pobj.columns||[]).join(', ')}`;
                  if(pobj && pobj.records){ try{ renderCandles(pobj.records); }catch(chartErr){ errorOut.classList.remove('hidden'); errorOut.textContent = 'Chart error: ' + chartErr; } }
                }
          }finally{
            try{ pres && pres.destroy && pres.destroy(); }catch(e){}
            try{ cleaned && cleaned.destroy && cleaned.destroy(); }catch(e){}
            try{ pyBridge && pyBridge.destroy && pyBridge.destroy(); }catch(e){}
            try{ pyPost && pyPost.destroy && pyPost.destroy(); }catch(e){}
          }
        }else if(res.type === 'cols' || res.type === 'cols_typed'){
          const cols = res.columns;
          const dtypes = res.dtypes || {};
          // For typed columns (Uint8Array buffers), unpack to Python memoryviews for zero-copy
          const pyCols = {};
          const allocated = [];
          try{
            for(const k of Object.keys(cols)){
              const dt = dtypes[k] || 'object';
              const v = cols[k];
              if(res.type === 'cols_typed' && dt !== 'object' && v && v instanceof Uint8Array){
                try{
                  const pyBuf = pyodide.unpack_buffer(v);
                  pyCols[k] = pyBuf;
                  allocated.push(pyBuf);
                }catch(e){
                  // fallback: pass JS array (shouldn't happen often)
                  pyCols[k] = Array.from(new Float64Array(v.buffer));
                }
              }else{
                pyCols[k] = v;
              }
            }
            // Convert JS mapping to Python dicts so Python can call .items() reliably
            let pyColsPy = null;
            let pyDtypesPy = null;
            let pres = null;
            let cleaned = null;
            let pyBridge = null;
            let pyPost = null;
            try{
                pyColsPy = pyodide.toPy(pyCols);
                pyDtypesPy = pyodide.toPy(dtypes || {});
                pyBridge = pyodide.globals.get('bridge');
                pyPost = pyodide.globals.get('postprocess');
                pres = await pyBridge.from_columns(pyColsPy, pyDtypesPy);
                cleaned = pyPost.chartify_records(pres);
                const pobj = cleaned.toJs ? cleaned.toJs() : cleaned;
                try{ displayDebug(pobj); }catch(e){}
                  if(pobj && pobj.error){
                  errorOut.classList.remove('hidden'); errorOut.textContent = pobj.error;
                }else{
                  if(dataOut) dataOut.textContent = JSON.stringify(pobj.head || [], null, 2);
                  if(resultOut) resultOut.textContent = `Rows: ${pobj.nrows || 'N/A'} | Columns: ${(pobj.columns||[]).join(', ')}`;
                  if(pobj && pobj.records){ try{ renderCandles(pobj.records); }catch(chartErr){ errorOut.classList.remove('hidden'); errorOut.textContent = 'Chart error: ' + chartErr; } }
                }
            }finally{
              try{ pyColsPy && pyColsPy.destroy && pyColsPy.destroy(); }catch(e){}
              try{ pyDtypesPy && pyDtypesPy.destroy && pyDtypesPy.destroy(); }catch(e){}
              try{ pres && pres.destroy && pres.destroy(); }catch(e){}
              try{ cleaned && cleaned.destroy && cleaned.destroy(); }catch(e){}
              try{ pyBridge && pyBridge.destroy && pyBridge.destroy(); }catch(e){}
              try{ pyPost && pyPost.destroy && pyPost.destroy(); }catch(e){}
            }
          }finally{
            try{ for(const a of allocated) a.destroy && a.destroy(); }catch(e){}
          }
        }else{
          throw new Error('Did not receive Arrow payload');
        }
      }catch(arrowErr){
        console.warn('pyarrow Arrow path failed, falling back to JS rows:', arrowErr);
        // fallthrough to rows handling below
        if(pathBanner){ pathBanner.style.display='block'; pathBanner.textContent = 'Data path: JS rows (pyarrow failed)'; }
        const rows = await queryParquetWithWorker(remoteUrl, start, end, false);
        await handleRowsPath(rows);
      }
    }else if(arrowLib === 'arro3'){
      // arro3 available in Pyodide: prefer JS rows and let Python (arro3/pandas) ingest them.
      if(pathBanner){ pathBanner.style.display='block'; pathBanner.textContent = `Data path: JS rows (arro3 available)`; }
      const rows = await queryParquetWithWorker(remoteUrl, start, end, false);
      await handleRowsPath(rows);
    }else{
      if(pathBanner){ pathBanner.style.display='block'; pathBanner.textContent = 'Data path: JS rows (no arrow library)'; }
      // Fallback: use JS rows and pass to Python via pyodide.toPy
      const rows = await queryParquetWithWorker(remoteUrl, start, end, false);
        if(!rows || rows.length === 0){
        if(dataOut) dataOut.textContent = 'No rows returned.';
        if(resultOut) resultOut.textContent = '—';
      }else{
        const norm = normalizeRows(rows);
        const head = norm.slice(0,10);
        const cols = Object.keys(head[0] || {});
        // send normalized rows to Python helper which will build a DataFrame
        const pyRows = pyodide.toPy(norm);
        let pres = null;
        let cleaned = null;
        let pyBridge = null;
        let pyPost = null;
        try{
          pyBridge = pyodide.globals.get('bridge');
          pyPost = pyodide.globals.get('postprocess');
          pres = await pyBridge.from_rows(pyRows);
          cleaned = pyPost.chartify_records(pres);
          const pobj = cleaned.toJs ? cleaned.toJs() : cleaned;
          try{ displayDebug(pobj); }catch(e){}
            if(pobj && pobj.error){
            errorOut.classList.remove('hidden'); errorOut.textContent = pobj.error;
          }else{
            if(dataOut) dataOut.textContent = JSON.stringify(pobj.head || head, null, 2);
            if(resultOut) resultOut.textContent = `Rows: ${pobj.nrows || norm.length} | Columns: ${(pobj.columns||cols).join(', ')}`;
            if(pobj && pobj.records){ try{ renderCandles(pobj.records); }catch(chartErr){ errorOut.classList.remove('hidden'); errorOut.textContent = 'Chart error: ' + chartErr; } }
          }
        }finally{
          try{ pyRows.destroy && pyRows.destroy(); }catch(e){}
          try{ pres && pres.destroy && pres.destroy(); }catch(e){}
          try{ cleaned && cleaned.destroy && cleaned.destroy(); }catch(e){}
          try{ pyBridge && pyBridge.destroy && pyBridge.destroy(); }catch(e){}
          try{ pyPost && pyPost.destroy && pyPost.destroy(); }catch(e){}
        }
      }
    }
  }catch(e){
    errorOut.classList.remove('hidden'); errorOut.textContent = String(e);
  }finally{
    hideSpinner();
    try{ duckWorker && duckWorker.terminate(); }catch(e){}
  }
}

// Centralized handler for JS rows -> Python backtest path
async function handleRowsPath(rows){
  if(!rows || rows.length === 0){
  if(dataOut) dataOut.textContent = 'No rows returned.';
    if(resultOut) resultOut.textContent = '—';
    return;
  }
  try{
    const norm = normalizeRows(rows);
    const head = norm.slice(0,10);
    const cols = Object.keys(head[0] || {});
    const pyRows = pyodide.toPy(norm);
    let pres = null;
    let cleaned = null;
    let pyBridge = null;
    let pyPost = null;
    try{
      pyBridge = pyodide.globals.get('bridge');
      pyPost = pyodide.globals.get('postprocess');
      pres = await pyBridge.from_rows(pyRows);
      cleaned = pyPost.chartify_records(pres);
      const pobj = cleaned.toJs ? cleaned.toJs() : cleaned;
      try{ displayDebug(pobj); }catch(e){}
        if(pobj && pobj.error){
        errorOut.classList.remove('hidden'); errorOut.textContent = pobj.error;
      }else{
        if(dataOut) dataOut.textContent = JSON.stringify(pobj.head || head, null, 2);
        if(resultOut) resultOut.textContent = `Rows: ${pobj.nrows || norm.length} | Columns: ${(pobj.columns||cols).join(', ')}`;
      }
    }finally{
      try{ pyRows.destroy && pyRows.destroy(); }catch(e){}
      try{ pres && pres.destroy && pres.destroy(); }catch(e){}
      try{ cleaned && cleaned.destroy && cleaned.destroy(); }catch(e){}
      try{ pyBridge && pyBridge.destroy && pyBridge.destroy(); }catch(e){}
      try{ pyPost && pyPost.destroy && pyPost.destroy(); }catch(e){}
    }
  }catch(e){
    errorOut.classList.remove('hidden'); errorOut.textContent = String(e);
  }
}

// Optional: wire a separate UI control to this path or replace the run() call
// For now keep the original run() and expose runWithWorker to console for testing.
