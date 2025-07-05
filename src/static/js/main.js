// Basic JavaScript for the analytics dashboard

document.addEventListener('DOMContentLoaded', function() {
    console.log('Analytics Dashboard loaded');
    
    // Add any interactive features here
    const tables = document.querySelectorAll('table');
    tables.forEach(table => {
        // Make tables more responsive
        table.classList.add('table-responsive');
    });
}); 