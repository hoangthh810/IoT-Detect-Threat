const slides = [...document.querySelectorAll('.slide')];
const dotsContainer = document.getElementById('dots');
const prevBtn = document.getElementById('prevBtn');
const nextBtn = document.getElementById('nextBtn');
const editBtn = document.getElementById('editBtn');
const fullscreenBtn = document.getElementById('fullscreenBtn');
const counter = document.getElementById('counter');
const progressBar = document.getElementById('progressBar');

let current = 0;
let editing = false;

slides.forEach((slide, index) => {
  const dot = document.createElement('button');
  dot.type = 'button';
  dot.className = 'dot';
  dot.setAttribute('aria-label', `Mở trang ${index + 1}: ${slide.dataset.title}`);
  dot.addEventListener('click', () => showSlide(index));
  dotsContainer.appendChild(dot);
});

const dots = [...dotsContainer.children];

function showSlide(index) {
  current = Math.max(0, Math.min(index, slides.length - 1));

  slides.forEach((slide, slideIndex) => {
    const active = slideIndex === current;
    slide.classList.toggle('active', active);
    slide.setAttribute('aria-hidden', String(!active));
  });

  dots.forEach((dot, dotIndex) => dot.classList.toggle('active', dotIndex === current));
  counter.textContent = `${current + 1} / ${slides.length}`;
  progressBar.style.width = `${((current + 1) / slides.length) * 100}%`;
  prevBtn.disabled = current === 0;
  nextBtn.disabled = current === slides.length - 1;
  history.replaceState(null, '', `#slide-${current + 1}`);
}

function fitDeck() {
  const controlsSpace = 88;
  const horizontalSpace = 28;
  const availableWidth = window.innerWidth - horizontalSpace;
  const availableHeight = window.innerHeight - controlsSpace;
  const scale = Math.min(availableWidth / 1600, availableHeight / 900, 1);
  document.documentElement.style.setProperty('--scale', scale.toFixed(4));
}

function toggleEditing() {
  editing = !editing;
  document.body.classList.toggle('editing', editing);
  editBtn.setAttribute('aria-pressed', String(editing));
  editBtn.textContent = editing ? 'Đang sửa' : 'Sửa chữ';

  document.querySelectorAll('[data-edit]').forEach((element) => {
    element.contentEditable = String(editing);
    element.spellcheck = editing;
  });
}

async function toggleFullscreen() {
  if (!document.fullscreenElement) {
    await document.documentElement.requestFullscreen?.();
  } else {
    await document.exitFullscreen?.();
  }
}

prevBtn.addEventListener('click', () => showSlide(current - 1));
nextBtn.addEventListener('click', () => showSlide(current + 1));
editBtn.addEventListener('click', toggleEditing);
fullscreenBtn.addEventListener('click', toggleFullscreen);

document.addEventListener('keydown', (event) => {
  if (editing && event.target.closest('[contenteditable="true"]')) return;

  if (['ArrowRight', 'PageDown', ' '].includes(event.key)) {
    event.preventDefault();
    showSlide(current + 1);
  }

  if (['ArrowLeft', 'PageUp'].includes(event.key)) {
    event.preventDefault();
    showSlide(current - 1);
  }

  if (event.key === 'Home') showSlide(0);
  if (event.key === 'End') showSlide(slides.length - 1);
  if (event.key.toLowerCase() === 'e') toggleEditing();
  if (event.key.toLowerCase() === 'f') toggleFullscreen();
});

window.addEventListener('resize', fitDeck);

const requestedSlide = Number.parseInt(location.hash.replace('#slide-', ''), 10);
showSlide(Number.isFinite(requestedSlide) ? requestedSlide - 1 : 0);
fitDeck();
