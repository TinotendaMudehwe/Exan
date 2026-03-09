const barCtx = document.getElementById('barChart');
const pieCtx = document.getElementById('pieChart');

new Chart(barCtx, {
type: 'bar',
data: {
labels: ['Jan','Feb','Mar','Apr'],
datasets: [{
label: 'Sales',
data: [12,19,8,15]
}]
}
});

new Chart(pieCtx, {
type: 'pie',
data: {
labels: ['Product A','Product B','Product C'],
datasets: [{
data: [30,40,30]
}]
}
});
