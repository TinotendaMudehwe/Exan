import React, { useState } from "react";
import "../App.css";
import filterImage from "../assets/excel-filter.png";
import { motion } from "framer-motion";

const API_BASE_URL = `http://${window.location.hostname}:5000`;

function LoginPage() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [statusMessage, setStatusMessage] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();
    setErrorMessage("");
    setStatusMessage("");
    setIsSubmitting(true);

    try {
      const response = await fetch(`${API_BASE_URL}/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: "include",
        body: JSON.stringify({ username, password, remember })
      });

      const contentType = response.headers.get("content-type") || "";
      const data = contentType.includes("application/json") ? await response.json() : {};

      if (!response.ok || !data.success) {
        setErrorMessage(data.message || "Login failed. Please try again.");
        return;
      }

      setStatusMessage("Login successful. Redirecting...");
      const redirectUrl = data.redirect_url || "/";
      window.location.href = `${API_BASE_URL}${redirectUrl}`;
    } catch (error) {
      setErrorMessage("Unable to reach the server. Ensure Flask is running on port 5000.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="login-page">
      <div className="falling-files" aria-hidden="true">
        <motion.div
          className="falling-file file1"
          initial={{ y: -120, opacity: 0 }}
          animate={{ y: "112vh", opacity: [0, 0.7, 0.7, 0] }}
          transition={{ duration: 11, repeat: Infinity, ease: "linear", delay: 0.2 }}
        >
          .xlsx
        </motion.div>
        <motion.div
          className="falling-file file2"
          initial={{ y: -150, opacity: 0 }}
          animate={{ y: "112vh", opacity: [0, 0.75, 0.75, 0] }}
          transition={{ duration: 13, repeat: Infinity, ease: "linear", delay: 1.8 }}
        >
          .xls
        </motion.div>
        <motion.div
          className="falling-file file3"
          initial={{ y: -180, opacity: 0 }}
          animate={{ y: "112vh", opacity: [0, 0.7, 0.7, 0] }}
          transition={{ duration: 15, repeat: Infinity, ease: "linear", delay: 2.9 }}
        >
          .csv
        </motion.div>
      </div>

      <div className="left-panel">
        <motion.img
          src={filterImage}
          alt="Excel Analysis Filter"
          className="filter-image"
          animate={{
            y: [0, -20, 0]
          }}
          transition={{
            duration: 6,
            repeat: Infinity,
            ease: "easeInOut"
          }}
        />

        <motion.div
          className="gear gear-large"
          animate={{ rotate: 360 }}
          transition={{ duration: 13, repeat: Infinity, ease: "linear" }}
        >
          ⚙
        </motion.div>

        <motion.div
          className="gear gear-small"
          animate={{ rotate: -360 }}
          transition={{ duration: 10, repeat: Infinity, ease: "linear" }}
        >
          ⚙
        </motion.div>

        <motion.div
          className="excel-icon icon1"
          animate={{ y: [0, -16, 0] }}
          transition={{ duration: 5.5, repeat: Infinity, ease: "easeInOut" }}
        >
          📄
        </motion.div>

        <motion.div
          className="excel-icon icon2 chart-glow"
          animate={{ y: [0, -14, 0] }}
          transition={{ duration: 6.5, repeat: Infinity, ease: "easeInOut" }}
        >
          📊
        </motion.div>

        <motion.div
          className="excel-icon icon3 chart-glow"
          animate={{ y: [0, -18, 0] }}
          transition={{ duration: 7.2, repeat: Infinity, ease: "easeInOut" }}
        >
          📈
        </motion.div>
      </div>

      <div className="right-panel">
        <motion.div
          className="login-card"
          initial={{ opacity: 0, x: 100 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 1 }}
        >
          <h1 className="system-title">Exan</h1>

          <h3>Login to Continue</h3>

          <form onSubmit={handleSubmit}>
            <input
              type="text"
              placeholder="Username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
            />

            <input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />

            <div className="remember">
              <label htmlFor="remember">Remember me</label>
              <input
                id="remember"
                type="checkbox"
                checked={remember}
                onChange={(e) => setRemember(e.target.checked)}
              />
            </div>

            {errorMessage ? <p className="form-message error">{errorMessage}</p> : null}
            {statusMessage ? <p className="form-message success">{statusMessage}</p> : null}

            <button type="submit" disabled={isSubmitting}>
              {isSubmitting ? "Signing In..." : "Sign In"}
            </button>
          </form>
        </motion.div>
      </div>
    </div>
  );
}

export default LoginPage;
