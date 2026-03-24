import React, { useEffect, useState } from "react";
import "./App.css";

const API_BASE = process.env.REACT_APP_API_BASE_URL || "http://localhost:8000";

function App() {
  const [table, setTable] = useState("silver");
  const [versions, setVersions] = useState([]);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [metadata, setMetadata] = useState(null);
  const [query, setQuery] = useState(
    "SELECT type, COUNT(*) AS event_count FROM table GROUP BY type ORDER BY event_count DESC LIMIT 5"
  );
  const [results, setResults] = useState([]);
  const [error, setError] = useState("");

  useEffect(() => {
    fetch(`${API_BASE}/api/tables/${table}/versions`)
      .then((res) => res.json())
      .then((data) => {
        setVersions(data);
        if (data.length > 0) {
          setSelectedVersion(data[0].version);
        }
      })
      .catch((err) => setError(String(err)));
  }, [table]);

  useEffect(() => {
    if (selectedVersion === null) return;
    fetch(`${API_BASE}/api/tables/${table}/versions/${selectedVersion}`)
      .then((res) => res.json())
      .then((data) => setMetadata(data))
      .catch((err) => setError(String(err)));
  }, [table, selectedVersion]);

  const runQuery = () => {
    setError("");
    fetch(`${API_BASE}/api/tables/${table}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, version: selectedVersion }),
    })
      .then((res) => {
        if (!res.ok) {
          return res.json().then((d) => {
            throw new Error(d.error || "Query failed");
          });
        }
        return res.json();
      })
      .then((data) => setResults(data))
      .catch((err) => setError(String(err)));
  };

  return (
    <div className="App">
      <header className="App-header">
        <h2>Lakehouse Explorer</h2>
      </header>
      <div className="App-container">
        <aside className="Sidebar">
          <div>
            <label>Table:&nbsp;</label>
            <select
              value={table}
              onChange={(e) => {
                setTable(e.target.value);
                setMetadata(null);
                setResults([]);
              }}
            >
              <option value="bronze">Bronze</option>
              <option value="silver">Silver</option>
              <option value="silver_corrected">Silver Corrected</option>
            </select>
          </div>
          <h4>Versions</h4>
          <ul className="Version-list">
            {versions.map((v) => (
              <li
                key={v.version}
                className={
                  selectedVersion === v.version ? "Version-item selected" : "Version-item"
                }
                onClick={() => setSelectedVersion(v.version)}
              >
                v{v.version} - {v.operation} -{" "}
                {new Date(v.timestamp).toLocaleString()}
              </li>
            ))}
          </ul>
        </aside>
        <main className="Main">
          <section className="Metadata">
            <h3>Metadata</h3>
            {metadata ? (
              <div>
                <p>Row count: {metadata.rowCount}</p>
                <h4>Schema</h4>
                <table>
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(metadata.schema).map(([col, typ]) => (
                      <tr key={col}>
                        <td>{col}</td>
                        <td>{typ}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p>No metadata loaded.</p>
            )}
          </section>

          <section className="Query">
            <h3>SQL Editor</h3>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              rows={6}
              cols={80}
            />
            <br />
            <button onClick={runQuery}>Run</button>
            {error && <p className="Error">Error: {error}</p>}
          </section>

          <section className="Results">
            <h3>Results</h3>
            {results.length === 0 ? (
              <p>No results.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    {Object.keys(results[0]).map((k) => (
                      <th key={k}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, idx) => (
                    <tr key={idx}>
                      {Object.keys(row).map((k) => (
                        <td key={k}>{String(row[k])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </main>
      </div>
    </div>
  );
}

export default App;
