async function loadComponent(path, targetId) {
  const response = await fetch(path);
  const html = await response.text();
  document.getElementById(targetId).innerHTML = html;
}

// Carica header e footer
document.addEventListener('DOMContentLoaded', () => {
  loadComponent('includes/header.html', 'header');
  loadComponent('includes/footer.html', 'footer');
  loadComponent('includes/menu.html', 'menu');
});