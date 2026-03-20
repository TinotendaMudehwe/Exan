(function () {
    var THEME_OPTIONS = ["auto", "dark", "light", "amber", "forest", "presentation"];

    function canUseMatchMedia() {
        return typeof window.matchMedia === "function";
    }

    function resolveAppliedTheme(themeChoice) {
        if (themeChoice === "auto") {
            if (canUseMatchMedia() && window.matchMedia("(prefers-color-scheme: light)").matches) {
                return "light";
            }
            return "dark";
        }
        return themeChoice;
    }

    function getSavedTheme() {
        try {
            var saved = localStorage.getItem("exan-theme") || "dark";
            if (THEME_OPTIONS.indexOf(saved) === -1) {
                return "dark";
            }
            return saved;
        } catch (error) {
            return "dark";
        }
    }

    function ensureManagedOptions(selectEl) {
        if (!selectEl) {
            return;
        }

        if (!selectEl.querySelector("option[value='auto']")) {
            var autoOption = document.createElement("option");
            autoOption.value = "auto";
            autoOption.textContent = "Auto";
            selectEl.insertBefore(autoOption, selectEl.firstChild);
        }

        if (!selectEl.querySelector("option[value='presentation']")) {
            var presentationOption = document.createElement("option");
            presentationOption.value = "presentation";
            presentationOption.textContent = "Presentation";
            selectEl.appendChild(presentationOption);
        }
    }

    function ensureThemeSwatches(selectEl) {
        if (!selectEl || !selectEl.parentElement) {
            return;
        }

        if (selectEl.parentElement.querySelector(".theme-swatch-strip")) {
            return;
        }

        var strip = document.createElement("div");
        strip.className = "theme-swatch-strip";

        ["dark", "light", "amber", "forest", "presentation"].forEach(function (themeName) {
            var dot = document.createElement("span");
            dot.className = "theme-swatch-dot theme-swatch-" + themeName;
            dot.setAttribute("data-theme-dot", themeName);
            dot.setAttribute("title", themeName.charAt(0).toUpperCase() + themeName.slice(1));
            strip.appendChild(dot);
        });

        selectEl.parentElement.appendChild(strip);
    }

    function updateSwatchState(themeChoice) {
        var active = resolveAppliedTheme(themeChoice);
        var allDots = document.querySelectorAll(".theme-swatch-dot");
        allDots.forEach(function (dot) {
            var isActive = dot.getAttribute("data-theme-dot") === active;
            dot.setAttribute("data-active", isActive ? "true" : "false");
        });
    }

    function applyTheme(themeChoice) {
        var safeChoice = THEME_OPTIONS.indexOf(themeChoice) >= 0 ? themeChoice : "dark";
        var appliedTheme = resolveAppliedTheme(safeChoice);

        document.body.setAttribute("data-theme-choice", safeChoice);
        document.body.setAttribute("data-theme", appliedTheme);
        updateSwatchState(safeChoice);

        var themeSelects = document.querySelectorAll(".theme-select");
        themeSelects.forEach(function (selectEl) {
            ensureManagedOptions(selectEl);
            ensureThemeSwatches(selectEl);
            selectEl.value = safeChoice;
        });

        try {
            localStorage.setItem("exan-theme", safeChoice);
        } catch (error) {
            // Ignore storage errors (private mode or policy restrictions).
        }
    }

    function initTheme() {
        var savedChoice = getSavedTheme();
        applyTheme(savedChoice);

        var themeSelects = document.querySelectorAll(".theme-select");
        themeSelects.forEach(function (selectEl) {
            ensureManagedOptions(selectEl);
            ensureThemeSwatches(selectEl);
            selectEl.addEventListener("change", function (event) {
                applyTheme(event.target.value);
            });
        });

        if (canUseMatchMedia()) {
            var media = window.matchMedia("(prefers-color-scheme: light)");
            var onSystemChange = function () {
                if (getSavedTheme() === "auto") {
                    applyTheme("auto");
                }
            };

            if (typeof media.addEventListener === "function") {
                media.addEventListener("change", onSystemChange);
            } else if (typeof media.addListener === "function") {
                media.addListener(onSystemChange);
            }
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initTheme);
    } else {
        initTheme();
    }
})();
