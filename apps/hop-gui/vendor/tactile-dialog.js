/* Async, queued confirmation/notice dialogs. Text is always text, never HTML. */
(() => {
  'use strict';
  let sequence = 0, queue = Promise.resolve();
  function present(message, options, question) {
    return new Promise(resolve => {
      const previous = document.activeElement;
      const dialog = document.createElement('dialog');
      const id = `t-decision-${++sequence}`;
      dialog.className = 't-dialog t-dialog--decision';
      dialog.dataset.tone = options.tone || (question ? 'danger' : 'info');
      dialog.setAttribute('aria-labelledby', `${id}-title`);
      dialog.setAttribute('aria-describedby', `${id}-body`);
      const head = document.createElement('div'); head.className = 't-dialog-head';
      const title = document.createElement('h2'); title.id = `${id}-title`;
      title.textContent = options.title || (question ? 'Even bevestigen' : 'Melding');
      const close = document.createElement('button'); close.type = 'button';
      close.className = 't-action t-action--neutral t-icon-button'; close.setAttribute('aria-label', 'Sluiten');
      const glyph = document.createElement('span'); glyph.className = 'material-symbols-outlined t-icon'; glyph.setAttribute('aria-hidden', 'true'); glyph.textContent = 'close'; close.append(glyph);
      head.append(title, close);
      const body = document.createElement('div'); body.id = `${id}-body`; body.className = 't-dialog-body'; body.textContent = String(message);
      const foot = document.createElement('div'); foot.className = 't-dialog-foot';
      const cancel = document.createElement('button'); cancel.type = 'button'; cancel.className = 't-action t-action--neutral'; cancel.textContent = options.cancelLabel || 'Annuleren';
      const accept = document.createElement('button'); accept.type = 'button'; accept.className = 't-action' + (dialog.dataset.tone === 'danger' ? ' t-action--danger' : ''); accept.textContent = options.confirmLabel || (question ? 'Doorgaan' : 'Begrepen');
      if (question) foot.append(cancel); foot.append(accept);
      dialog.append(head, body, foot); document.body.append(dialog);
      let result = false;
      const finish = value => { result = value; dialog.close(); };
      cancel.addEventListener('click', () => finish(false));
      close.addEventListener('click', () => finish(false));
      accept.addEventListener('click', () => finish(true));
      // Consume Escape here so the browser never closes a parent work window.
      dialog.addEventListener('keydown', event => {
        if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); finish(false); }
      });
      dialog.addEventListener('cancel', event => { event.preventDefault(); finish(false); });
      dialog.addEventListener('close', () => {
        dialog.remove();
        if (previous?.isConnected) previous.focus({preventScroll:true});
        resolve(result);
      }, {once:true});
      dialog.showModal(); (question ? cancel : accept).focus();
    });
  }
  function enqueue(message, options, question) {
    const next = queue.then(() => present(message, options, question));
    queue = next.catch(() => {}); return next;
  }
  globalThis.TactileDialog = Object.freeze({
    confirm: (message, options = {}) => enqueue(message, options, true),
    notice: (message, options = {}) => enqueue(message, options, false)
  });
})();
