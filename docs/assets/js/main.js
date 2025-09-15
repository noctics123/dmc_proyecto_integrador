// ========================================
// DMC Pipeline - Interactive JavaScript
// ========================================

document.addEventListener('DOMContentLoaded', function() {
    // Initialize all components
    initNavigation();
    initTabs();
    initAnimations();
    initChart();
    initScrollEffects();
    initFAQs();

    // Initialize Lucide icons
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }

    // Initialize AOS animations
    if (typeof AOS !== 'undefined') {
        AOS.init({
            duration: 600,
            easing: 'ease-out-cubic',
            once: true,
            offset: 100
        });
    }
});

// ========================================
// Navigation
// ========================================
function initNavigation() {
    const navbar = document.getElementById('navbar');
    const navToggle = document.getElementById('nav-toggle');
    const navMenu = document.getElementById('nav-menu');
    const navLinks = document.querySelectorAll('.nav-link');

    // Mobile menu toggle
    if (navToggle && navMenu) {
        navToggle.addEventListener('click', () => {
            navMenu.classList.toggle('active');
            navToggle.classList.toggle('active');
        });
    }

    // Close mobile menu when clicking on links
    navLinks.forEach(link => {
        link.addEventListener('click', () => {
            navMenu.classList.remove('active');
            navToggle.classList.remove('active');
        });
    });

    // Navbar scroll effect
    let lastScrollY = window.scrollY;

    window.addEventListener('scroll', () => {
        const currentScrollY = window.scrollY;

        if (navbar) {
            if (currentScrollY > 100) {
                navbar.style.background = 'rgba(255, 255, 255, 0.98)';
                navbar.style.boxShadow = '0 2px 20px rgba(0, 0, 0, 0.1)';
            } else {
                navbar.style.background = 'rgba(255, 255, 255, 0.95)';
                navbar.style.boxShadow = 'none';
            }
        }

        lastScrollY = currentScrollY;
    });

    // Active link highlighting based on scroll position
    updateActiveNavLink();
    window.addEventListener('scroll', debounce(updateActiveNavLink, 100));
}

function updateActiveNavLink() {
    const sections = document.querySelectorAll('section[id]');
    const navLinks = document.querySelectorAll('.nav-link');

    let current = '';
    const scrollY = window.scrollY;

    sections.forEach(section => {
        const sectionTop = section.offsetTop - 100;
        const sectionHeight = section.offsetHeight;

        if (scrollY >= sectionTop && scrollY < sectionTop + sectionHeight) {
            current = section.getAttribute('id');
        }
    });

    navLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === `#${current}`) {
            link.classList.add('active');
        }
    });
}

// ========================================
// Tabs System
// ========================================
function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetTab = button.getAttribute('data-tab');

            // Remove active classes
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));

            // Add active classes
            button.classList.add('active');
            const targetContent = document.getElementById(`${targetTab}-tab`);
            if (targetContent) {
                targetContent.classList.add('active');

                // Trigger animation for new content
                const cards = targetContent.querySelectorAll('.service-detail-card');
                cards.forEach((card, index) => {
                    card.style.opacity = '0';
                    card.style.transform = 'translateY(20px)';

                    setTimeout(() => {
                        card.style.transition = 'all 0.3s ease-out';
                        card.style.opacity = '1';
                        card.style.transform = 'translateY(0)';
                    }, index * 100);
                });
            }
        });
    });
}

// ========================================
// Animations
// ========================================
function initAnimations() {
    // Hero data flow animation
    animateDataFlow();

    // Intersection Observer for scroll animations
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('fade-in-up');
            }
        });
    }, observerOptions);

    // Observe elements for animation
    const animateElements = document.querySelectorAll('.metric-card, .service-detail-card, .component-category, .doc-card');
    animateElements.forEach(el => observer.observe(el));
}

function animateDataFlow() {
    const flowNodes = document.querySelectorAll('.flow-node');

    if (flowNodes.length === 0) return;

    let currentIndex = 0;

    function animateNext() {
        // Reset all nodes
        flowNodes.forEach(node => {
            node.style.transform = 'scale(1)';
            node.style.background = 'rgba(255, 255, 255, 0.2)';
        });

        // Animate current node
        const currentNode = flowNodes[currentIndex];
        currentNode.style.transform = 'scale(1.1)';
        currentNode.style.background = 'rgba(255, 255, 255, 0.4)';

        currentIndex = (currentIndex + 1) % flowNodes.length;
    }

    // Start animation
    animateNext();
    setInterval(animateNext, 1500);
}

// ========================================
// Chart Initialization
// ========================================
function initChart() {
    const chartCanvas = document.getElementById('pipelineChart');

    if (!chartCanvas || typeof Chart === 'undefined') return;

    const ctx = chartCanvas.getContext('2d');

    // Sample data representing pipeline metrics
    const chartData = {
        labels: ['Landing', 'Bronze', 'Silver', 'Gold'],
        datasets: [{
            label: 'Registros Procesados (Miles)',
            data: [676, 676, 110, 5],
            backgroundColor: [
                'rgba(66, 133, 244, 0.8)',
                'rgba(251, 188, 5, 0.8)',
                'rgba(156, 163, 175, 0.8)',
                'rgba(52, 168, 83, 0.8)'
            ],
            borderColor: [
                'rgb(66, 133, 244)',
                'rgb(251, 188, 5)',
                'rgb(156, 163, 175)',
                'rgb(52, 168, 83)'
            ],
            borderWidth: 2,
            borderRadius: 8,
            borderSkipped: false
        }]
    };

    const chartConfig = {
        type: 'bar',
        data: chartData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Volumen de Datos por Capa del Lakehouse',
                    font: {
                        size: 18,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: 'white',
                    bodyColor: 'white',
                    cornerRadius: 8,
                    displayColors: false,
                    callbacks: {
                        label: function(context) {
                            const value = context.parsed.y;
                            if (context.dataIndex === 0 || context.dataIndex === 1) {
                                return `${value}K registros SIMBAD`;
                            } else if (context.dataIndex === 2) {
                                return `${value}K registros limpios`;
                            } else {
                                return `${value}K métricas agregadas`;
                            }
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.1)'
                    },
                    ticks: {
                        callback: function(value) {
                            return value + 'K';
                        }
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            },
            animation: {
                duration: 2000,
                easing: 'easeOutQuart'
            }
        }
    };

    // Set canvas height
    chartCanvas.style.height = '400px';

    new Chart(ctx, chartConfig);
}

// ========================================
// Scroll Effects
// ========================================
function initScrollEffects() {
    // Parallax effect for hero section
    const hero = document.querySelector('.hero');
    const heroParticles = document.querySelector('.hero-particles');

    if (hero && heroParticles) {
        window.addEventListener('scroll', () => {
            const scrolled = window.scrollY;
            const rate = scrolled * -0.5;

            heroParticles.style.transform = `translateY(${rate}px)`;
        });
    }

    // Counter animation for metrics
    animateCounters();
}

function animateCounters() {
    const counterElements = document.querySelectorAll('.metric-value');

    const observerOptions = {
        threshold: 0.5
    };

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                animateCounter(entry.target);
                observer.unobserve(entry.target);
            }
        });
    }, observerOptions);

    counterElements.forEach(el => observer.observe(el));
}

function animateCounter(element) {
    const text = element.textContent.trim();
    const isTime = text.includes('<');
    const isPercentage = text.includes('%');

    if (isTime || text.includes('K')) {
        // Don't animate time or complex text
        return;
    }

    const target = parseInt(text.replace(/[^\d]/g, ''));
    if (isNaN(target)) return;

    let current = 0;
    const increment = target / 60; // 60 frames for 1 second at 60fps

    function updateCounter() {
        current += increment;

        if (current >= target) {
            element.textContent = text; // Reset to original text
        } else {
            const value = Math.floor(current);
            if (isPercentage) {
                element.textContent = value + '%';
            } else {
                element.textContent = value.toLocaleString();
            }
            requestAnimationFrame(updateCounter);
        }
    }

    updateCounter();
}

// ========================================
// Utility Functions
// ========================================

// Debounce function to limit function calls
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

// Smooth scroll for anchor links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));

        if (target) {
            const offsetTop = target.offsetTop - 80; // Account for fixed navbar

            window.scrollTo({
                top: offsetTop,
                behavior: 'smooth'
            });
        }
    });
});

// Loading state management
window.addEventListener('load', function() {
    document.body.classList.add('loaded');

    // Hide any loading spinners
    const loaders = document.querySelectorAll('.loader');
    loaders.forEach(loader => {
        loader.style.display = 'none';
    });
});

// Error handling for external resources
window.addEventListener('error', function(e) {
    console.warn('Resource failed to load:', e.target.src || e.target.href);

    // Handle missing images
    if (e.target.tagName === 'IMG') {
        e.target.style.display = 'none';
    }
});

// Performance monitoring
if ('performance' in window) {
    window.addEventListener('load', function() {
        setTimeout(function() {
            const perfData = performance.getEntriesByType('navigation')[0];
            console.log('Page load time:', perfData.loadEventEnd - perfData.loadEventStart + 'ms');
        }, 0);
    });
}

// ========================================
// Theme Toggle (Future Enhancement)
// ========================================
function initThemeToggle() {
    const themeToggle = document.getElementById('theme-toggle');

    if (!themeToggle) return;

    // Check for saved theme preference
    const savedTheme = localStorage.getItem('theme');
    const systemTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    const currentTheme = savedTheme || systemTheme;

    document.documentElement.setAttribute('data-theme', currentTheme);

    themeToggle.addEventListener('click', function() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem('theme', newTheme);
    });
}

// ========================================
// Service Worker Registration (Future)
// ========================================
if ('serviceWorker' in navigator) {
    window.addEventListener('load', function() {
        // Future: Register service worker for offline functionality
        // navigator.serviceWorker.register('/sw.js');
    });
}

// ========================================
// FAQs Functionality
// ========================================
function initFAQs() {
    const faqItems = document.querySelectorAll('.faq-item');
    
    faqItems.forEach(item => {
        const question = item.querySelector('.faq-question');
        
        if (question) {
            question.addEventListener('click', () => {
                // Close other open FAQs
                faqItems.forEach(otherItem => {
                    if (otherItem !== item) {
                        otherItem.classList.remove('active');
                    }
                });
                
                // Toggle current FAQ
                item.classList.toggle('active');
            });
        }
    });
}

// Export functions for external use
window.DMCPipeline = {
    initNavigation,
    initTabs,
    initAnimations,
    initChart,
    animateCounters,
    debounce,
    initFAQs
};