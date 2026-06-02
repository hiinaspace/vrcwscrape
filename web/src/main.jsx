import React from "react";
import ReactDOM from "react-dom/client";
// Self-hosted Inter so the SDF atlas rasterizes the same glyphs in every browser
// (otherwise each browser picks a different fallback — some with an extreme "J").
import "@fontsource/inter/400.css";
import "@fontsource/inter/700.css";
import App from "./App.jsx";
import "./style.css";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
