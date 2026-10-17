/* Runs before stylesheets so stored preferences are applied before first paint. */
(() => {
  'use strict';
  const key = 'hop-appearance';
  const defaults = { style: '08', color: 'plum', mode: 'dark' };
  const palettes = {
    plum: { glossy: '#811b76', classic: '#800080', matte: '#67509a', light: '#cfbcff', glossyLight: '#edb8e6' },
    red: { glossy: '#a40000', classic: '#800000', matte: '#a93632', light: '#ffb4ab', glossyLight: '#ffb4ae' },
    blue: { glossy: '#00449e', classic: '#000080', matte: '#365c9a', light: '#adc8ff', glossyLight: '#abcfff' },
    petrol: { glossy: '#225050', classic: '#008080', matte: '#246b65', light: '#91d5cf', glossyLight: '#a5d8d3' },
    green: { glossy: '#29623b', classic: '#008000', matte: '#326a47', light: '#a6dab4', glossyLight: '#b1dfb6' }
  };
  function valid(value) {
    return {
      style: ['95', '08', 'matte'].includes(value?.style) ? value.style : defaults.style,
      color: Object.hasOwn(palettes, value?.color) ? value.color : defaults.color,
      mode: ['dark', 'light'].includes(value?.mode) ? value.mode : defaults.mode
    };
  }
  let current = defaults;
  try { current = valid(JSON.parse(localStorage.getItem(key))); } catch (_) {}
  function apply(value, save = false) {
    current = valid(value);
    const root = document.documentElement, colors = palettes[current.color];
    const light = current.mode === 'light', matte = current.style === 'matte';
    const accent = current.style === '95' ? colors.classic : matte ? (light ? colors.matte : colors.light) : colors.glossy;
    root.dataset.style = current.style;
    root.dataset.theme = current.mode;
    root.style.setProperty('--accent', accent);
    root.style.setProperty('--accent-light', light ? accent : matte ? colors.light : colors.glossyLight);
    root.style.setProperty('--accent-ink', matte && !light ? '#241a35' : '#fff');
    if (save) {
      let message = 'Your preference is saved in this browser.';
      try { localStorage.setItem(key, JSON.stringify(current)); } catch (_) { message = 'Your preference applies to this page; browser storage is unavailable.'; }
      const status = document.getElementById('appearance-status');
      if (status) status.textContent = message;
    }
    sync();
    window.dispatchEvent(new CustomEvent('hop-appearance-change', { detail: { ...current } }));
  }
  function sync() {
    for (const field of ['style', 'color']) {
      const select = document.getElementById(`appearance-${field}`);
      if (select) { select.value = current[field]; window.TactileSelect?.get(select)?.refresh(); }
    }
    document.querySelectorAll('[data-appearance-mode]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.appearanceMode === current.mode)));
  }
  apply(current);
  document.addEventListener('DOMContentLoaded', () => {
    sync();
    const dialog = document.getElementById('appearance-dialog');
    document.querySelectorAll('[data-open-appearance]').forEach(button => button.addEventListener('click', () => dialog.showModal()));
    document.querySelectorAll('[data-close-appearance]').forEach(button => button.addEventListener('click', () => dialog.close()));
    for (const field of ['style', 'color']) document.getElementById(`appearance-${field}`).addEventListener('change', event => apply({ ...current, [field]: event.target.value }, true));
    document.querySelectorAll('[data-appearance-mode]').forEach(button => button.addEventListener('click', () => apply({ ...current, mode: button.dataset.appearanceMode }, true)));
  }, { once: true });
  window.addEventListener('storage', event => {
    if (event.key !== key) return;
    try { apply(JSON.parse(event.newValue)); } catch (_) { apply(defaults); }
  });
})();
