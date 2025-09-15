// SEO and Meta Tags Enhancement
document.addEventListener('DOMContentLoaded', function() {
    // Update meta tags dynamically based on section
    updateMetaTags();

    // Track page interactions for analytics
    trackUserInteractions();

    // Add structured data for better SEO
    addStructuredData();
});

function updateMetaTags() {
    // Base meta information
    const siteInfo = {
        title: 'DMC Pipeline - Sistema Integral de Análisis de Datos',
        description: 'Pipeline completo de análisis de créditos hipotecarios SIMBAD e indicadores macroeconómicos usando arquitectura lakehouse híbrida en Google Cloud Platform.',
        url: 'https://noctics123.github.io/dmc_proyecto_integrador',
        image: 'https://noctics123.github.io/dmc_proyecto_integrador/docs/assets/images/logo.svg',
        keywords: 'BigQuery, DataProc, SIMBAD, Pipeline, Lakehouse, Google Cloud, Análisis de Datos, República Dominicana'
    };

    // Update existing meta tags
    updateMetaTag('description', siteInfo.description);
    updateMetaTag('keywords', siteInfo.keywords);

    // Open Graph tags
    updateMetaProperty('og:title', siteInfo.title);
    updateMetaProperty('og:description', siteInfo.description);
    updateMetaProperty('og:url', siteInfo.url);
    updateMetaProperty('og:image', siteInfo.image);
    updateMetaProperty('og:type', 'website');
    updateMetaProperty('og:site_name', 'DMC Pipeline');

    // Twitter Card tags
    updateMetaName('twitter:card', 'summary_large_image');
    updateMetaName('twitter:title', siteInfo.title);
    updateMetaName('twitter:description', siteInfo.description);
    updateMetaName('twitter:image', siteInfo.image);

    // Additional SEO tags
    updateMetaName('robots', 'index, follow');
    updateMetaName('author', 'DMC Team');
    updateMetaName('theme-color', '#4285f4');
}

function updateMetaTag(name, content) {
    let meta = document.querySelector(`meta[name="${name}"]`);
    if (!meta) {
        meta = document.createElement('meta');
        meta.name = name;
        document.head.appendChild(meta);
    }
    meta.content = content;
}

function updateMetaProperty(property, content) {
    let meta = document.querySelector(`meta[property="${property}"]`);
    if (!meta) {
        meta = document.createElement('meta');
        meta.setAttribute('property', property);
        document.head.appendChild(meta);
    }
    meta.content = content;
}

function updateMetaName(name, content) {
    let meta = document.querySelector(`meta[name="${name}"]`);
    if (!meta) {
        meta = document.createElement('meta');
        meta.name = name;
        document.head.appendChild(meta);
    }
    meta.content = content;
}

function addStructuredData() {
    const structuredData = {
        "@context": "https://schema.org",
        "@type": "SoftwareApplication",
        "name": "DMC Pipeline",
        "description": "Sistema completo de análisis de créditos hipotecarios SIMBAD e indicadores macroeconómicos usando arquitectura lakehouse híbrida en Google Cloud Platform",
        "url": "https://noctics123.github.io/dmc_proyecto_integrador",
        "applicationCategory": "Data Analytics",
        "operatingSystem": "Web Browser",
        "programmingLanguage": ["Python", "SQL", "JavaScript"],
        "author": {
            "@type": "Organization",
            "name": "DMC Team"
        },
        "offers": {
            "@type": "Offer",
            "price": "0",
            "priceCurrency": "USD"
        },
        "screenshot": "https://noctics123.github.io/dmc_proyecto_integrador/docs/assets/images/logo.svg"
    };

    const script = document.createElement('script');
    script.type = 'application/ld+json';
    script.textContent = JSON.stringify(structuredData);
    document.head.appendChild(script);
}

function trackUserInteractions() {
    // Track navigation clicks
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', function() {
            // Analytics tracking would go here
            console.log('Navigation:', this.getAttribute('href'));
        });
    });

    // Track tab interactions
    document.querySelectorAll('.tab-btn').forEach(button => {
        button.addEventListener('click', function() {
            console.log('Tab viewed:', this.getAttribute('data-tab'));
        });
    });

    // Track documentation clicks
    document.querySelectorAll('.doc-card').forEach(card => {
        card.addEventListener('click', function() {
            console.log('Documentation accessed:', this.getAttribute('href'));
        });
    });
}