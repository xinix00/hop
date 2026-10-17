/* Tactile select: styled single/multiple listbox; the select owns form data. */
(function (global) {
  'use strict';
  const instances = new Map();
  let sequence = 0;
  const selectProto = HTMLSelectElement.prototype;
  const optionProto = HTMLOptionElement.prototype;

  function mount(root = document) {
    const sources = [...root.querySelectorAll('select:not([data-select-native])')];
    if (root.matches?.('select:not([data-select-native])')) sources.unshift(root);
    return sources.map(source => {
      if (instances.has(source)) return instances.get(source);
      const uid = `t-select-${++sequence}`;
      const wrapper = document.createElement('span');
      wrapper.className = `t-select ${source.className}`;
      source.before(wrapper); wrapper.append(source);
      const tabIndex = source.getAttribute('tabindex');
      source.classList.add('t-select-source'); source.tabIndex = -1;
      const trigger = document.createElement('button');
      if (tabIndex !== null) trigger.tabIndex = Number(tabIndex);
      trigger.type = 'button'; trigger.className = 't-select-trigger'; trigger.id = `${uid}-trigger`;
      trigger.setAttribute('role', 'combobox'); trigger.setAttribute('aria-haspopup', 'listbox');
      trigger.setAttribute('aria-expanded', 'false'); trigger.setAttribute('aria-controls', `${uid}-list`);
      const caption = document.createElement('span'); caption.className = 't-select-caption';
      const captionIcon = document.createElement('span'); captionIcon.className = 'material-symbols-outlined t-icon t-select-icon'; captionIcon.setAttribute('aria-hidden', 'true');
      const captionText = document.createElement('span'); captionText.className = 't-select-text';
      caption.append(captionIcon, captionText);
      const arrow = document.createElement('span'); arrow.className = 't-select-arrow t-action t-icon-button'; arrow.setAttribute('aria-hidden', 'true');
      const symbol = document.createElement('span'); symbol.className = 'material-symbols-outlined t-icon'; symbol.textContent = 'expand_more';
      arrow.append(symbol);
      trigger.append(caption, arrow);
      const popup = document.createElement('div'); popup.className = 't-select-popup'; popup.id = `${uid}-popup`; popup.popover = 'auto';
      const search = document.createElement('input'); search.type = 'search'; search.className = 't-select-search'; search.placeholder = 'Zoeken…';
      search.setAttribute('role', 'combobox'); search.setAttribute('aria-autocomplete', 'list'); search.setAttribute('aria-expanded', 'false');
      search.setAttribute('aria-label', 'Opties zoeken'); search.setAttribute('aria-controls', `${uid}-list`); search.autocomplete = 'off';
      const list = document.createElement('div'); list.id = `${uid}-list`; list.className = 't-select-list'; list.setAttribute('role', 'listbox');
      const empty = document.createElement('p'); empty.className = 't-select-empty'; empty.textContent = 'Geen opties beschikbaar.'; empty.setAttribute('role', 'status');
      popup.append(search, list, empty);
      const error = document.createElement('span'); error.className = 't-select-error'; error.id = `${uid}-error`; error.hidden = true; error.setAttribute('aria-live', 'polite');
      wrapper.append(trigger, popup, error);
      const abort = new AbortController(), restorers = new Map();
      const on = (node, event, fn, options = {}) => node.addEventListener(event, fn, { ...options, signal: abort.signal });
      let active = -1, structural = '', pending = false, destroyed = false, typed = '', typeTimer, rows = [];
      const isOpen = () => popup.matches(':popover-open');
      const disabled = option => option.disabled || (option.parentElement instanceof HTMLOptGroupElement && option.parentElement.disabled);
      const options = () => [...source.options];
      const available = () => rows.filter(row => !row.hidden && row.getAttribute('aria-disabled') !== 'true');
      function schedule() { if (!pending && !destroyed) { pending = true; queueMicrotask(() => { pending = false; if (!destroyed) refresh(); }); } }
      function watchProperty(object, property, prototype) {
        const descriptor = Object.getOwnPropertyDescriptor(prototype, property);
        const own = Object.getOwnPropertyDescriptor(object, property);
        if (!descriptor?.set || own?.configurable === false) return;
        Object.defineProperty(object, property, { configurable: true, get() { return descriptor.get.call(this); }, set(value) { descriptor.set.call(this, value); schedule(); } });
        const restore = () => own ? Object.defineProperty(object, property, own) : delete object[property];
        const entries = restorers.get(object) || []; entries.push(restore); restorers.set(object, entries);
      }
      watchProperty(source, 'value', selectProto); watchProperty(source, 'selectedIndex', selectProto);
      function setActive(index, scroll = false) {
        active = index;
        rows.forEach((row, i) => { row.dataset.active = String(i === active); });
        const row = rows[active];
        if (row) { trigger.setAttribute('aria-activedescendant', row.id); search.setAttribute('aria-activedescendant', row.id); if (scroll) row.scrollIntoView({ block: 'nearest' }); }
        else { trigger.removeAttribute('aria-activedescendant'); search.removeAttribute('aria-activedescendant'); }
      }
      function filter() {
        const query = search.value.trim().toLocaleLowerCase(), opts = options();
        rows.forEach((row, i) => { row.hidden = opts[i].hidden || opts[i].parentElement.hidden || !opts[i].label.toLocaleLowerCase().includes(query); });
        list.querySelectorAll('.t-select-group').forEach(group => {
          let node = group.nextElementSibling, visible = false;
          while (node && !node.classList.contains('t-select-group')) { visible ||= !node.hidden; node = node.nextElementSibling; }
          group.hidden = !visible;
        });
        empty.hidden = rows.some(row => !row.hidden);
        empty.textContent = rows.length ? 'Geen opties gevonden.' : 'Geen opties beschikbaar.';
        if (!rows[active] || rows[active].hidden || rows[active].getAttribute('aria-disabled') === 'true') setActive(Number(available()[0]?.dataset.index ?? -1));
      }
      function accessibleName() {
        const label = source.labels?.[0] || wrapper.previousElementSibling?.closest('label');
        if (source.getAttribute('aria-label')) return source.getAttribute('aria-label');
        if (source.getAttribute('aria-labelledby')) return source.getAttribute('aria-labelledby').split(/\s+/).map(id => document.getElementById(id)?.textContent || '').join(' ').trim();
        if (label) { const copy = label.cloneNode(true); copy.querySelectorAll('select,.t-select,input,button,small').forEach(node => node.remove()); return copy.textContent.trim(); }
        return source.title || source.name || 'Keuze';
      }
      function refresh() {
        const opts = options();
        restorers.forEach((restore, object) => { if (object !== source && !opts.includes(object)) { restore.forEach(fn => fn()); restorers.delete(object); } });
        opts.forEach(option => { if (!restorers.has(option)) watchProperty(option, 'selected', optionProto); });
        const name = accessibleName();
        trigger.disabled = source.matches(':disabled');
        wrapper.hidden = source.hidden || source.style.display === 'none';
        trigger.title = source.title;
        trigger.setAttribute('aria-required', String(source.required));
        trigger.setAttribute('aria-describedby', [source.getAttribute('aria-describedby'), error.id].filter(Boolean).join(' '));
        const selected = opts.filter(option => option.selected);
        const placeholder = source.dataset.placeholder || (source.multiple ? 'Kies opties…' : 'Maak een keuze…');
        captionIcon.textContent = source.dataset.icon || '';
        captionIcon.hidden = !source.dataset.icon;
        captionText.textContent = selected.length ? selected.map(option => option.label).join(', ') : placeholder;
        trigger.setAttribute('aria-label', `${name}: ${captionText.textContent}`);
        trigger.dataset.placeholder = String(!selected.length || (!source.multiple && selected[0]?.value === ''));
        list.setAttribute('aria-label', name); list.setAttribute('aria-multiselectable', String(source.multiple));
        search.hidden = !source.hasAttribute('data-search') && opts.length < 9;
        popup.dataset.multiple = String(source.multiple);
        const key = JSON.stringify(opts.map(option => [option.value, option.label, disabled(option), option.parentElement instanceof HTMLOptGroupElement ? option.parentElement.label : '', option.dataset.description || '']));
        if (key !== structural) {
          structural = key; list.replaceChildren(); rows = []; let lastGroup = null;
          opts.forEach((option, index) => {
            const group = option.parentElement instanceof HTMLOptGroupElement ? option.parentElement : null;
            if (group && group !== lastGroup) { const title = document.createElement('div'); title.className = 't-select-group'; title.textContent = group.label; list.append(title); }
            lastGroup = group;
            const row = document.createElement('div'); row.className = 't-select-option t-choice t-choice--marked'; row.id = `${uid}-option-${index}`;
            row.dataset.index = index; row.setAttribute('role', 'option'); row.setAttribute('aria-disabled', String(disabled(option)));
            const mark = document.createElement('span'); mark.className = 't-select-mark'; mark.setAttribute('aria-hidden', 'true');
            const text = document.createElement('span'); text.className = 't-select-option-label'; text.textContent = option.label;
            row.append(mark, text);
            if (option.dataset.description) { const detail = document.createElement('span'); detail.className = 't-select-detail'; detail.textContent = option.dataset.description; row.append(detail); }
            list.append(row); rows.push(row);
          });
        }
        rows.forEach((row, index) => row.setAttribute('aria-selected', String(opts[index].selected)));
        if (source.validity.valid) { error.hidden = true; trigger.removeAttribute('aria-invalid'); }
        filter();
        if (!isOpen()) { trigger.removeAttribute('aria-activedescendant'); search.removeAttribute('aria-activedescendant'); }
        if (isOpen()) { if (trigger.disabled || wrapper.hidden || !trigger.getClientRects().length) close(false); else position(); }
      }
      function position() {
        if (!isOpen()) return;
        const rect = trigger.getBoundingClientRect(), margin = 12;
        const width = Math.min(Math.max(rect.width, 250), innerWidth - margin * 2);
        const below = innerHeight - rect.bottom - 8 - margin, above = rect.top - 8 - margin;
        const upwards = below < 200 && above > below;
        popup.style.width = `${width}px`; popup.style.maxHeight = `${Math.max(80, Math.min(360, upwards ? above : below))}px`;
        const left = Math.min(Math.max(margin, rect.left), innerWidth - width - margin);
        popup.style.left = `${left}px`;
        popup.style.top = `${upwards ? Math.max(margin, rect.top - popup.getBoundingClientRect().height - 8) : rect.bottom + 8}px`;
      }
      function open() {
        refresh(); if (trigger.disabled || wrapper.hidden || !trigger.getClientRects().length) return;
        search.value = ''; filter();
        if (!isOpen()) popup.showPopover();
        trigger.setAttribute('aria-expanded', 'true');
        setActive(Number((available().find(row => row.getAttribute('aria-selected') === 'true') || available()[0])?.dataset.index ?? -1));
        position();
        search.setAttribute('aria-expanded', 'true');
        if (!search.hidden) search.focus({ preventScroll: true });
      }
      function close(focus = true) {
        if (isOpen()) popup.hidePopover();
        trigger.setAttribute('aria-expanded', 'false'); trigger.removeAttribute('aria-activedescendant');
        search.setAttribute('aria-expanded', 'false'); search.removeAttribute('aria-activedescendant');
        if (focus && source.isConnected) trigger.focus();
      }
      function choose(index) {
        const option = options()[index]; if (!option || disabled(option) || source.matches(':disabled')) return;
        if (source.multiple) option.selected = !option.selected; else source.selectedIndex = index;
        source.dispatchEvent(new Event('input', { bubbles: true })); source.dispatchEvent(new Event('change', { bubbles: true }));
        refresh(); if (source.multiple) setActive(index); else close();
      }
      function keyboard(event) {
        if (event.key === 'Tab') { close(false); return; }
        if (event.key === 'Escape' && isOpen()) { event.preventDefault(); event.stopPropagation(); close(); return; }
        if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
          event.preventDefault(); if (!isOpen()) { open(); return; }
          const candidates = available(); let index = candidates.findIndex(row => Number(row.dataset.index) === active);
          if (event.key === 'Home') index = 0;
          else if (event.key === 'End') index = candidates.length - 1;
          else index = Math.max(0, Math.min(candidates.length - 1, index + (event.key === 'ArrowDown' ? 1 : -1)));
          setActive(Number(candidates[index]?.dataset.index ?? -1), true); return;
        }
        if (event.key === 'Enter' || (event.key === ' ' && event.target !== search)) { event.preventDefault(); isOpen() ? choose(active) : open(); return; }
        if (event.target !== search && event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
          event.preventDefault(); if (!isOpen()) open(); typed += event.key.toLocaleLowerCase(); clearTimeout(typeTimer); typeTimer = setTimeout(() => { typed = ''; }, 700);
          const match = available().find(row => options()[Number(row.dataset.index)].label.toLocaleLowerCase().startsWith(typed));
          if (match) setActive(Number(match.dataset.index), true);
        }
      }
      on(trigger, 'click', () => { isOpen() ? close() : open(); });
      on(trigger, 'keydown', keyboard); on(popup, 'keydown', keyboard);
      on(list, 'mousedown', event => event.preventDefault());
      on(list, 'click', event => { const row = event.target.closest('[role=option]'); if (row) choose(Number(row.dataset.index)); });
      on(search, 'input', filter);
      on(popup, 'toggle', () => { trigger.setAttribute('aria-expanded', String(isOpen())); search.setAttribute('aria-expanded', String(isOpen())); if (!isOpen()) { trigger.removeAttribute('aria-activedescendant'); search.removeAttribute('aria-activedescendant'); } });
      on(source, 'input', schedule); on(source, 'change', schedule);
      on(source, 'focus', () => trigger.focus());
      on(source, 'invalid', event => { event.preventDefault(); error.textContent = source.validationMessage; error.hidden = false; trigger.setAttribute('aria-invalid', 'true'); trigger.focus(); });
      for (const label of source.labels || []) on(label, 'click', event => { if (!event.target.closest('.t-select')) { event.preventDefault(); trigger.focus(); } });
      if (source.form) on(source.form, 'reset', event => queueMicrotask(() => { if (!event.defaultPrevented) { refresh(); error.hidden = true; trigger.removeAttribute('aria-invalid'); close(false); } }));
      on(global, 'resize', position); on(document, 'scroll', event => { if (!popup.contains(event.target)) position(); }, { capture: true, passive: true });
      const observer = new MutationObserver(schedule);
      observer.observe(source, { childList: true, subtree: true, characterData: true, attributes: true });
      const api = { refresh, open, close, trigger, destroy() {
        destroyed = true; close(false); abort.abort(); observer.disconnect(); clearTimeout(typeTimer);
        restorers.forEach(restore => restore.forEach(fn => fn())); instances.delete(source);
        source.classList.remove('t-select-source'); if (tabIndex === null) source.removeAttribute('tabindex'); else source.setAttribute('tabindex', tabIndex);
        if (source.isConnected) wrapper.replaceWith(source); else wrapper.remove();
      } };
      instances.set(source, api); refresh(); return api;
    });
  }
  const observer = new MutationObserver(records => {
    records.forEach(record => {
      record.addedNodes.forEach(node => { if (node.nodeType === 1) mount(node); });
      if (record.type === 'attributes' && record.target instanceof HTMLFieldSetElement) record.target.querySelectorAll('select').forEach(source => instances.get(source)?.refresh());
    });
    instances.forEach((api, source) => { if (!source.isConnected) api.destroy(); });
  });
  function start() { mount(); observer.observe(document.documentElement, { childList: true, subtree: true, attributes: true, attributeFilter: ['disabled'] }); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true }); else start();
  global.TactileSelect = { mount, get: source => instances.get(source) };
})(window);
