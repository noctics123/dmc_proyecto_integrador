// ========================================
// DMC Pipeline - Interactive JavaScript
// ========================================

document.addEventListener('DOMContentLoaded', function() {
    // Initialize all components
    initNavigation();
    initTabs();
    initAOS();
    initChart();
    initScrollEffects();
    initFAQs();
    initArchModal(); // Final, comprehensive modal logic
});

function initAOS() {
    if (typeof AOS !== 'undefined') {
        AOS.init({
            duration: 600,
            easing: 'ease-out-cubic',
            once: true,
            offset: 100
        });
    }
}

function initNavigation() {
    const navbar = document.getElementById('navbar');
    const navToggle = document.getElementById('nav-toggle');
    const navMenu = document.getElementById('nav-menu');

    if (navToggle && navMenu) {
        navToggle.addEventListener('click', () => navMenu.classList.toggle('active'));
    }

    window.addEventListener('scroll', () => {
        if (navbar) {
            navbar.classList.toggle('scrolled', window.scrollY > 100);
        }
    });
}

function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetTab = button.getAttribute('data-tab');
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            button.classList.add('active');
            document.getElementById(`${targetTab}-tab`).classList.add('active');
        });
    });
}

function initChart() {
    const chartCanvas = document.getElementById('pipelineChart');
    if (!chartCanvas || typeof Chart === 'undefined') return;
    new Chart(chartCanvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['Landing', 'Bronze', 'Silver', 'Gold'],
            datasets: [{
                label: 'Registros Procesados (Miles)',
                data: [676, 676, 110, 5],
                backgroundColor: ['#06b6d4', '#f59e0b', '#9ca3af', '#facc15']
            }]
        },
        options: { responsive: true, plugins: { legend: { display: false } } }
    });
}

function initScrollEffects() {
    const heroParticles = document.querySelector('.hero-particles');
    if (heroParticles) {
        window.addEventListener('scroll', () => {
            heroParticles.style.transform = `translateY(${window.scrollY * -0.5}px)`;
        });
    }
}

function initFAQs() {
    const faqItems = document.querySelectorAll('.faq-item');
    faqItems.forEach(item => {
        const question = item.querySelector('.faq-question');
        if (question) {
            question.addEventListener('click', () => {
                const wasActive = item.classList.contains('active');
                faqItems.forEach(other => other.classList.remove('active'));
                if (!wasActive) item.classList.add('active');
            });
        }
    });
}

// ========================================
// Architecture Modal (Final Version)
// ========================================
function initArchModal() {
    const modal = document.getElementById('arch-modal');
    const btn = document.getElementById('expand-arch-btn');
    const span = document.getElementById('arch-modal-close');
    if (!modal || !btn || !span) return;

    const svg = modal.querySelector('svg');
    const kpiPanel = svg.getElementById('kpi-panel');
    const kpiToggle = svg.getElementById('kpi-panel-toggle');
    const tooltip = svg.getElementById('svg-tooltip');
    const tooltipTitle = svg.getElementById('svg-tooltip-title');
    const tooltipDesc = svg.getElementById('svg-tooltip-desc');
    const tooltipCost = svg.getElementById('svg-tooltip-cost');

    const allFlowElements = svg.querySelectorAll('[data-flow]');

    // --- Open/Close Modal --- //
    btn.onclick = () => {
        modal.classList.add('visible');
        document.body.style.overflow = 'hidden';
        svg.classList.add('animate');
        // Stagger animations
        svg.querySelectorAll('.interactive-svg.animate .interactive-group').forEach((el, i) => {
            el.style.animationDelay = `${0.5 + i * 0.05}s`;
        });
        svg.querySelectorAll('.interactive-svg.animate .flow-path').forEach((el, i) => {
            el.style.animationDelay = `${0.8 + i * 0.1}s`;
        });
    };

    const closeModal = () => {
        modal.classList.remove('visible');
        document.body.style.overflow = 'auto';
        svg.classList.remove('animate');
        resetHighlights();
    };

    span.onclick = closeModal;
    window.addEventListener('keydown', (e) => e.key === 'Escape' && closeModal());
    modal.addEventListener('click', (e) => e.target === modal && closeModal());

    // --- KPI Panel Toggle --- //
    kpiToggle.addEventListener('click', (e) => {
        e.stopPropagation();
        kpiPanel.classList.toggle('closed');
    });

    // --- Tooltip Logic --- //
    svg.querySelectorAll('.interactive-group').forEach(group => {
        group.addEventListener('mousemove', (e) => {
            tooltipTitle.textContent = group.getAttribute('data-title') || '';
            tooltipDesc.textContent = group.getAttribute('data-desc') || '';
            tooltipCost.textContent = group.getAttribute('data-cost') || '';

            const pt = svg.createSVGPoint();
            pt.x = e.clientX;
            pt.y = e.clientY;
            const svgP = pt.matrixTransform(svg.getScreenCTM().inverse());
            tooltip.setAttribute('transform', `translate(${svgP.x + 20}, ${svgP.y - 90})`);
            tooltip.style.opacity = '1';
        });
        group.addEventListener('mouseleave', () => tooltip.style.opacity = '0');
    });

    // --- Click to Highlight Flow --- //
    const resetHighlights = () => {
        allFlowElements.forEach(el => el.classList.remove('dimmed', 'highlighted'));
        svg.onclick = null;
    };

    svg.querySelectorAll('.interactive-group[data-flow]').forEach(el => {
        el.addEventListener('click', (e) => {
            e.stopPropagation();
            const flow = el.getAttribute('data-flow');
            if (!flow) return;

            allFlowElements.forEach(elem => {
                const elFlows = elem.getAttribute('data-flow').split(',');
                if (elFlows.includes(flow)) {
                    elem.classList.add('highlighted');
                    elem.classList.remove('dimmed');
                } else {
                    elem.classList.add('dimmed');
                    elem.classList.remove('highlighted');
                }
            });
            svg.onclick = resetHighlights;
        });
    });
}

// ========================================
// Utility Functions
// ========================================
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}