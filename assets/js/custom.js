// Inizializza componenti di Forty
document.addEventListener('DOMContentLoaded', () => {
  // Attiva animazioni scroll
  AOS.init({
    offset: 100,
    once: true
  });

  // Header con overlay (come nel template originale)
  const header = document.getElementById('header');
  header.style.backgroundColor = 'rgba(0, 51, 102, 0.9)';
});