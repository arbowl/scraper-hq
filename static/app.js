const root = document.getElementById('root');
const statusEl = document.getElementById('status');
const tabsList = document.getElementById('tabs-list');
const tabsContent = document.getElementById('tabs-content');
const totalSourcesEl = document.getElementById('total-sources');

// State: templates and buffers
let templates = {}; // name -> descriptor
let buffers = {};   // template -> stream -> [rows]
let tabs = {};      // template -> tab info
let activeTab = null;

function el(tag, attrs = {}, ...children) {
  const e = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === 'class') e.className = v; else if (k === 'html') e.innerHTML = v; else e.setAttribute(k, v);
  });
  for (const c of children) {
    if (typeof c === 'string') {
      try {
        // Sanitize the string to remove invalid characters
        const sanitized = c.replace(/[\u0000-\u001F\u007F-\u009F]/g, '');
        e.appendChild(document.createTextNode(sanitized));
      } catch (err) {
        // If still fails, use a safe fallback
        e.appendChild(document.createTextNode(''));
      }
    } else if (c) {
      e.appendChild(c);
    }
  }
  return e;
}

function createTab(templateName) {
  if (tabs[templateName]) return tabs[templateName];
  
  // Create tab button
  const tab = el('div', {class: 'tab'}, templateName);
  const count = el('span', {class: 'tab-count'}, '0');
  tab.appendChild(count);
  
  // Create tab panel
  const panel = el('div', {class: 'tab-panel'});
  
  // Store tab info
  tabs[templateName] = { tab, panel, count };
  
  // Add click handler
  tab.addEventListener('click', () => switchTab(templateName));
  
  // Add to DOM
  tabsList.appendChild(tab);
  tabsContent.appendChild(panel);
  
  // Set as active if first tab
  if (!activeTab) {
    switchTab(templateName);
  }
  
  return tabs[templateName];
}

function switchTab(templateName) {
  // Remove active class from all tabs and panels
  Object.values(tabs).forEach(({tab, panel}) => {
    tab.classList.remove('active');
    panel.classList.remove('active');
  });
  
  // Add active class to selected tab and panel
  const selectedTab = tabs[templateName];
  if (selectedTab) {
    selectedTab.tab.classList.add('active');
    selectedTab.panel.classList.add('active');
    activeTab = templateName;
  }
}

function updateTabCount(templateName) {
  const tabInfo = tabs[templateName];
  if (tabInfo) {
    const count = Object.keys(buffers[templateName] || {}).length;
    tabInfo.count.textContent = count;
  }
  updateTotalSourcesCount();
}

function updateTotalSourcesCount() {
  const total = Object.values(buffers).reduce((sum, streams) => sum + Object.keys(streams).length, 0);
  totalSourcesEl.textContent = `${total} source${total !== 1 ? 's' : ''}`;
}

async function ensureCard(templateName, stream) {
  const id = `${templateName}::${stream}`;
  let card = document.querySelector(`[data-id="${id}"]`);
  if (card) return card;

  const tpl = document.getElementById('table-template');
  card = tpl.content.firstElementChild.cloneNode(true);
  card.dataset.id = id;
  card.dataset.template = templateName;
  card.dataset.stream = stream;
  
  // header title
  try {
    card.querySelector('.card-title').textContent = `${templateName} — ${stream}`;
  } catch (err) {
    card.querySelector('.card-title').textContent = `${templateName} — ${stream}`.replace(/[\u0000-\u001F\u007F-\u009F]/g, '');
  }

  // table header
  const thead = card.querySelector('thead');
  const tdesc = templates[templateName];
  if (tdesc && tdesc.columns) {
    const tr = el('tr');
    tdesc.columns.forEach(col => {
      tr.appendChild(el('th', {}, col.label));
    });
    thead.appendChild(tr);
  }

  // collapse button functionality
  const collapseBtn = card.querySelector('.collapse-btn');
  const cardContent = card.querySelector('.card-content');
  
  const toggleCollapse = () => {
    const isCollapsed = cardContent.classList.contains('collapsed');
    if (isCollapsed) {
      cardContent.classList.remove('collapsed');
      collapseBtn.classList.remove('collapsed');
      collapseBtn.textContent = '▼';
      card.classList.remove('collapsed');
    } else {
      cardContent.classList.add('collapsed');
      collapseBtn.classList.add('collapsed');
      collapseBtn.textContent = '▶';
      card.classList.add('collapsed');
    }
  };
  
  collapseBtn.addEventListener('click', toggleCollapse);
  collapseBtn.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      toggleCollapse();
    }
  });

  // summarize button
  card.querySelector('.summarize').addEventListener('click', async () => {
    const pre = card.querySelector('.insights');
    pre.hidden = false;
    pre.textContent = 'Generating summary...';
    pre.classList.add('typing');
    
    try {
      const resp = await fetch('/summarize', {
        method: 'POST', 
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ 
          template: templateName, 
          stream, 
          limit: 40 
        })
      });
      
      if (!resp.ok) {
        throw new Error(`HTTP error! status: ${resp.status}`);
      }
      
      // Handle streaming response
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let fullText = '';
      
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        const chunk = decoder.decode(value);
        const lines = chunk.split('\n');
        
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.token) {
                fullText += data.token;
                pre.textContent = fullText;
              } else if (data.error) {
                pre.textContent = `Error: ${data.error}`;
                break;
              }
            } catch (e) {
              // Skip malformed JSON lines
            }
          }
        }
      }
      
    } catch (e) {
      pre.textContent = `Error: ${e.message}`;
    } finally {
      pre.classList.remove('typing');
    }
  });

  // custom query functionality
  const queryInput = card.querySelector('.query-input');
  const querySuggestions = card.querySelector('.query-suggestions');
  const customQueryBtn = card.querySelector('.custom-query');
  
  // Load past queries for suggestions
  let pastQueries = [];
  try {
    const resp = await fetch('/queries');
    pastQueries = await resp.json();
  } catch (e) {
    console.warn('Failed to load past queries:', e);
  }

  // Show suggestions when typing
  queryInput.addEventListener('input', (e) => {
    const query = e.target.value.toLowerCase();
    if (query.length < 2) {
      querySuggestions.hidden = true;
      return;
    }
    
    const matches = pastQueries.filter(q => 
      q.toLowerCase().includes(query)
    ).slice(0, 8);
    
    if (matches.length > 0) {
      querySuggestions.innerHTML = '';
      matches.forEach((match, index) => {
        const div = document.createElement('div');
        div.className = 'query-suggestion';
        div.textContent = match;
        div.setAttribute('data-index', index);
        div.addEventListener('click', () => {
          queryInput.value = match;
          querySuggestions.hidden = true;
        });
        querySuggestions.appendChild(div);
      });
      querySuggestions.hidden = false;
    } else {
      querySuggestions.hidden = true;
    }
  });

  // Keyboard navigation for suggestions
  let selectedIndex = -1;
  queryInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      if (querySuggestions.hidden || selectedIndex === -1) {
        customQueryBtn.click();
      } else {
        // Select the highlighted suggestion
        const suggestions = querySuggestions.querySelectorAll('.query-suggestion');
        if (suggestions[selectedIndex]) {
          queryInput.value = suggestions[selectedIndex].textContent;
          querySuggestions.hidden = true;
          selectedIndex = -1;
        }
      }
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (!querySuggestions.hidden) {
        const suggestions = querySuggestions.querySelectorAll('.query-suggestion');
        selectedIndex = Math.min(selectedIndex + 1, suggestions.length - 1);
        updateSelection();
      }
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (!querySuggestions.hidden) {
        const suggestions = querySuggestions.querySelectorAll('.query-suggestion');
        selectedIndex = Math.max(selectedIndex - 1, -1);
        updateSelection();
      }
    } else if (e.key === 'Escape') {
      querySuggestions.hidden = true;
      selectedIndex = -1;
    }
  });

  function updateSelection() {
    const suggestions = querySuggestions.querySelectorAll('.query-suggestion');
    suggestions.forEach((s, i) => {
      if (i === selectedIndex) {
        s.style.backgroundColor = 'rgba(31, 111, 235, 0.2)';
        s.style.borderLeft = '3px solid #1f6feb';
      } else {
        s.style.backgroundColor = '';
        s.style.borderLeft = '';
      }
    });
  }

  // Hide suggestions when clicking outside
  document.addEventListener('click', (e) => {
    if (!queryInput.contains(e.target) && !querySuggestions.contains(e.target)) {
      querySuggestions.hidden = true;
    }
  });

  // Handle custom query submission
  customQueryBtn.addEventListener('click', async () => {
    const query = queryInput.value.trim();
    if (!query) return;
    
    // Store the query
    try {
      await fetch('/queries', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ query })
      });
    } catch (e) {
      console.warn('Failed to store query:', e);
    }
    
    // Get data and send to LLM with streaming
    const pre = card.querySelector('.insights');
    pre.hidden = false;
    pre.textContent = 'Generating response...';
    pre.classList.add('typing');
    
    try {
      const resp = await fetch('/summarize', {
        method: 'POST', 
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ 
          template: templateName, 
          stream, 
          limit: 40,
          prompt: query 
        })
      });
      
      if (!resp.ok) {
        throw new Error(`HTTP error! status: ${resp.status}`);
      }
      
      // Handle streaming response
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let fullText = '';
      
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        const chunk = decoder.decode(value);
        const lines = chunk.split('\n');
        
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.token) {
                fullText += data.token;
                pre.textContent = fullText;
              } else if (data.error) {
                pre.textContent = `Error: ${data.error}`;
                break;
              }
            } catch (e) {
              // Skip malformed JSON lines
            }
          }
        }
      }
      
    } catch (e) {
      pre.textContent = `Error: ${e.message}`;
    } finally {
      pre.classList.remove('typing');
    }
    
    // Clear input and hide suggestions
    queryInput.value = '';
    querySuggestions.hidden = true;
  });

  // Handle Enter key in query input
  queryInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      customQueryBtn.click();
    }
  });

  // Get or create tab for this template
  const tabInfo = createTab(templateName);
  
  // Add card to the tab panel instead of root
  tabInfo.panel.appendChild(card);
  return card;
}

function renderRows(card, templateName, rows) {
  const tableWrap = card.querySelector('.table-wrap');
  if (!tableWrap) {
    console.error('No table-wrap found in card');
    console.error('Card HTML:', card.outerHTML);
    return;
  }
  
  // Clear existing content
  tableWrap.innerHTML = '';
  
  if (!rows || rows.length === 0) {
    tableWrap.innerHTML = '<div style="padding: 20px; text-align: center; color: var(--muted);">No data available</div>';
    return;
  }
  
  // Get template columns
  const tdesc = templates[templateName];
  if (!tdesc || !tdesc.columns) {
    tableWrap.innerHTML = '<div style="padding: 20px; text-align: center; color: var(--muted);">Template not found</div>';
    return;
  }
  
  // Create table
  const table = el('table');
  const thead = el('thead');
  const tbody = el('tbody');
  
  // Create header using template columns
  const headerRow = el('tr');
  tdesc.columns.forEach(col => {
    const th = el('th', {}, col.label);
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  
  // Create rows using template columns
  rows.forEach((row, rowIndex) => {
    const tr = el('tr');
    tdesc.columns.forEach(col => {
      const td = el('td');
      const value = row[col.key];
      
      try {
        if (col.key === 'url' || col.key === 'agenda_url') {
          // Handle URL fields - show as clickable links if they exist
          if (value && value !== '') {
            const link = el('a', {href: String(value), target: '_blank'}, 'View');
            link.style.color = '#7fb0ff';
            link.style.textDecoration = 'none';
            td.appendChild(link);
          } else {
            td.textContent = '—';
          }
        } else {
          // Regular text content - ensure it's a safe string
          td.textContent = String(value || '');
        }
      } catch (err) {
        // If anything fails, show a safe fallback
        td.textContent = '—';
      }
      
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  
  table.appendChild(thead);
  table.appendChild(tbody);
  tableWrap.appendChild(table);
}

async function applyChunk(chunk) {
  const { template, stream, rows, mode } = chunk;
  if (!buffers[template]) buffers[template] = {};
  if (!buffers[template][stream] || mode === 'replace') buffers[template][stream] = [];
  buffers[template][stream].push(...rows);
  
  // Ensure tab exists for this template
  createTab(template);
  
  const card = await ensureCard(template, stream);
  
  // Auto-expand when new data arrives
  const cardContent = card.querySelector('.card-content');
  const collapseBtn = card.querySelector('.collapse-btn');
  if (cardContent.classList.contains('collapsed')) {
    cardContent.classList.remove('collapsed');
    collapseBtn.classList.remove('collapsed');
    collapseBtn.textContent = '▼';
    card.classList.remove('collapsed');
  }
  
  // Update row count in title
  const cardTitle = card.querySelector('.card-title');
  if (cardTitle) {
    const totalRows = buffers[template][stream].length;
    cardTitle.textContent = `${template} — ${stream} (${totalRows} rows)`;
  }
  
  // Render all rows for this stream, not just the new ones
  renderRows(card, template, buffers[template][stream]);
  updateTabCount(template);
}

async function boot() {
  // load templates
  const t = await (await fetch('/templates')).json();
  templates = Object.fromEntries(t.map(td => [td.template, td]));

  // load snapshot
  const snap = await (await fetch('/snapshot')).json();
  buffers = snap;

  // Create tabs for all existing templates
  Object.keys(templates).forEach(template => {
    createTab(template);
  });

  // materialize existing
  for (const [template, streams] of Object.entries(buffers)) {
    for (const stream of Object.keys(streams)) {
      const card = await ensureCard(template, stream);
      const rows = (buffers[template]?.[stream]) || [];
      renderRows(card, template, rows);
    }
    updateTabCount(template);
  }

  // SSE
  const es = new EventSource('/events');
  es.onopen = () => statusEl.textContent = 'Live';
  es.onerror = () => statusEl.textContent = 'Reconnecting…';
  es.onmessage = async (ev) => {
    try {
      const data = JSON.parse(ev.data);
      if (data.type === 'hello') return;
      await applyChunk(data);
    } catch (e) { 
      console.error('Error processing SSE data:', e);
    }
  };
}

boot();