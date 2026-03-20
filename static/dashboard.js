(function () {
    function showLoadingOverlay() {
        var overlay = document.querySelector("[data-loading-overlay]");
        if (overlay) {
            overlay.hidden = false;
        }
    }

    function hideLoadingOverlay() {
        var overlay = document.querySelector("[data-loading-overlay]");
        if (overlay) {
            overlay.hidden = true;
        }
    }

    function pushToast(message, tone) {
        if (!message) {
            return;
        }

        var tray = document.querySelector("[data-toast-tray]");
        if (!tray) {
            return;
        }

        var toast = document.createElement("div");
        toast.className = "toast toast-" + (tone || "info");
        toast.textContent = message;
        tray.appendChild(toast);

        window.setTimeout(function () {
            toast.classList.add("toast-visible");
        }, 20);

        window.setTimeout(function () {
            toast.classList.remove("toast-visible");
            window.setTimeout(function () {
                toast.remove();
            }, 220);
        }, 2600);
    }

    function initFormLoading() {
        var forms = document.querySelectorAll("form");
        forms.forEach(function (form) {
            form.addEventListener("submit", function () {
                showLoadingOverlay();
            });
        });
    }

    function initBlock01Search() {
        var input = document.querySelector("[data-block01-search]");
        var table = document.querySelector("[data-block01-table]");
        if (!input || !table) {
            return;
        }

        var rows = Array.prototype.slice.call(table.querySelectorAll("tbody tr"));
        input.addEventListener("input", function (event) {
            var query = String(event.target.value || "").trim().toLowerCase();
            rows.forEach(function (row) {
                var haystack = String(row.getAttribute("data-search") || "").toLowerCase();
                row.hidden = query && haystack.indexOf(query) === -1;
            });
        });
    }

    function initExportToasts() {
        var exportButtons = document.querySelectorAll("[data-export-trigger]");
        exportButtons.forEach(function (button) {
            button.addEventListener("click", function () {
                pushToast("Preparing report export...", "info");
            });
        });
    }

    function initChartGalleryControls() {
        var controlBars = document.querySelectorAll("[data-chart-controls]");
        controlBars.forEach(function (controlBar) {
            var section = controlBar.closest("section");
            var gallery = section ? section.querySelector("[data-chart-gallery]") : null;
            if (!gallery) {
                return;
            }

            var searchInput = controlBar.querySelector("[data-chart-search]");
            var sortSelect = controlBar.querySelector("[data-chart-sort]");
            var limitSelect = controlBar.querySelector("[data-chart-limit]");
            var compactToggle = controlBar.querySelector("[data-chart-compact]");

            function getCards() {
                return Array.prototype.slice.call(gallery.querySelectorAll("[data-chart-card]"));
            }

            function loadVisibleCards() {
                var cards = getCards();

                cards.forEach(function (card) {
                    if (card.hidden) {
                        return;
                    }

                    var chartKey = card.getAttribute("data-chart-key");
                    var chartFrame = card.querySelector("[data-chart-frame]");

                    if (!chartKey || !chartFrame) {
                        return;
                    }

                    if (card.getAttribute("data-chart-loaded") === "true" || card.getAttribute("data-chart-loading") === "true") {
                        return;
                    }

                    card.setAttribute("data-chart-loading", "true");

                    fetch("/advanced/chart-fragment/" + encodeURIComponent(chartKey), {
                        credentials: "same-origin"
                    })
                        .then(function (response) {
                            if (!response.ok) {
                                throw new Error("Chart fetch failed");
                            }
                            return response.json();
                        })
                        .then(function (payload) {
                            chartFrame.innerHTML = payload && payload.html ? payload.html : "No chart data available.";
                            card.setAttribute("data-chart-loaded", "true");
                            card.removeAttribute("data-chart-loading");

                            var lazyNode = chartFrame.querySelector(".plotly-lazy");
                            if (lazyNode) {
                                renderLazyPlotlyChart(lazyNode);
                            }
                        })
                        .catch(function () {
                            chartFrame.textContent = "Unable to load chart right now.";
                            card.removeAttribute("data-chart-loading");
                        });
                });
            }

            function applyState() {
                var cards = getCards();
                var query = searchInput ? String(searchInput.value || "").trim().toLowerCase() : "";
                var sortMode = sortSelect ? sortSelect.value : "default";
                var limitValue = limitSelect ? limitSelect.value : "all";
                var visibleLimit = limitValue === "all" ? Number.MAX_SAFE_INTEGER : Math.max(1, parseInt(limitValue, 10) || 1);

                var matchingCards = cards.filter(function (card) {
                    var title = String(card.getAttribute("data-chart-title") || "").toLowerCase();
                    var matched = !query || title.indexOf(query) !== -1;
                    card.hidden = !matched;
                    return matched;
                });

                matchingCards.sort(function (leftCard, rightCard) {
                    var leftTitle = String(leftCard.getAttribute("data-chart-title") || "");
                    var rightTitle = String(rightCard.getAttribute("data-chart-title") || "");
                    var leftOrder = parseInt(leftCard.getAttribute("data-chart-order") || "0", 10);
                    var rightOrder = parseInt(rightCard.getAttribute("data-chart-order") || "0", 10);

                    if (sortMode === "az") {
                        return leftTitle.localeCompare(rightTitle);
                    }
                    if (sortMode === "za") {
                        return rightTitle.localeCompare(leftTitle);
                    }
                    return leftOrder - rightOrder;
                });

                matchingCards.forEach(function (card, index) {
                    gallery.appendChild(card);
                    card.hidden = index >= visibleLimit;
                });

                gallery.classList.toggle("charts-grid-compact", !!(compactToggle && compactToggle.checked));
                loadVisibleCards();
            }

            [searchInput, sortSelect, limitSelect, compactToggle].forEach(function (control) {
                if (!control) {
                    return;
                }
                var eventName = control.tagName === "INPUT" && control.type === "search" ? "input" : "change";
                control.addEventListener(eventName, applyState);
            });

            var showMoreButton = section ? section.querySelector("[data-chart-show-more]") : null;
            if (showMoreButton && limitSelect) {
                showMoreButton.addEventListener("click", function () {
                    var cards = getCards();
                    var currentLimit = limitSelect.value === "all" ? cards.length : Math.max(1, parseInt(limitSelect.value, 10) || 1);

                    if (currentLimit >= cards.length) {
                        limitSelect.value = "all";
                        showMoreButton.disabled = true;
                        showMoreButton.textContent = "All charts loaded";
                    } else if (currentLimit < 4 && limitSelect.querySelector("option[value='4']")) {
                        limitSelect.value = "4";
                    } else {
                        limitSelect.value = "all";
                    }

                    limitSelect.dispatchEvent(new Event("change"));
                });
            }

            applyState();
        });
    }

    function renderLazyPlotlyChart(container) {
        if (!container || container.getAttribute("data-loaded") === "true") {
            return true;
        }

        if (typeof window.Plotly === "undefined") {
            return false;
        }

        var payloadText = container.getAttribute("data-plotly");
        if (!payloadText) {
            return false;
        }

        var payload;
        try {
            payload = JSON.parse(payloadText);
        } catch (error) {
            return false;
        }

        container.innerHTML = "";

        var target = document.createElement("div");
        target.className = "plotly-lazy-target";
        container.appendChild(target);

        window.Plotly.newPlot(
            target,
            payload.data || [],
            payload.layout || {},
            {
                responsive: true,
                displaylogo: false
            }
        );

        container.setAttribute("data-loaded", "true");
        container.removeAttribute("data-plotly");
        return true;
    }

    function initLazyPlotlyCharts() {
        var lazyCharts = Array.prototype.slice.call(document.querySelectorAll(".plotly-lazy"));
        if (!lazyCharts.length) {
            return;
        }

        function startObserver() {
            if (typeof window.Plotly === "undefined") {
                return false;
            }

            if (!("IntersectionObserver" in window)) {
                lazyCharts.forEach(function (chart) {
                    renderLazyPlotlyChart(chart);
                });
                return true;
            }

            var observer = new IntersectionObserver(function (entries) {
                entries.forEach(function (entry) {
                    if (!entry.isIntersecting) {
                        return;
                    }

                    var rendered = renderLazyPlotlyChart(entry.target);
                    if (rendered) {
                        observer.unobserve(entry.target);
                    }
                });
            }, {
                rootMargin: "320px 0px",
                threshold: 0.01
            });

            lazyCharts.forEach(function (chart) {
                observer.observe(chart);
            });

            return true;
        }

        if (startObserver()) {
            return;
        }

        var waitAttempts = 0;
        var waitTimer = window.setInterval(function () {
            waitAttempts += 1;

            if (startObserver() || waitAttempts > 50) {
                window.clearInterval(waitTimer);
            }
        }, 100);
    }

    function initPageNotice() {
        var page = document.body;
        if (!page) {
            return;
        }
        var notice = page.getAttribute("data-page-notice");
        if (notice) {
            pushToast(notice, "success");
        }
    }

    function initMobileDrawers() {
        if (window.innerWidth > 980) {
            return;
        }
        var drawers = document.querySelectorAll("details.control-drawer");
        drawers.forEach(function (drawer, index) {
            drawer.open = index === 0;
        });
    }

    function init() {
        hideLoadingOverlay();
        initLazyPlotlyCharts();
        initFormLoading();
        initBlock01Search();
        initExportToasts();
        initChartGalleryControls();
        initPageNotice();
        initMobileDrawers();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
