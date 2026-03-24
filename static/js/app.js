
document.querySelectorAll('.js-copy').forEach(btn => {
  btn.addEventListener('click', async () => {
    const id = btn.getAttribute('data-copy-target');
    const el = document.getElementById(id);
    if (!el) return;
    const text = el.innerText.trim();
    try {
      await navigator.clipboard.writeText(text);
      const old = btn.textContent;
      btn.textContent = 'Copied';
      setTimeout(() => btn.textContent = old, 1200);
    } catch (e) {
      alert('Copy failed. Please copy manually.');
    }
  });
});
