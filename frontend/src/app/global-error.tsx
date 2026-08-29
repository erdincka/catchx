"use client";

/**
 * Root error boundary.
 *
 * Replaces Next's built-in fallback, which renders outside the provider tree.
 * It carries its own inline styles for the same reason — at this point the
 * theme provider may not have mounted, so nothing here may depend on it.
 */
export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html lang="en-GB">
      <body
        style={{
          margin: 0,
          minHeight: "100dvh",
          display: "grid",
          placeItems: "center",
          padding: "2rem",
          fontFamily: "ui-sans-serif, system-ui, -apple-system, sans-serif",
          background: "#fbfbfa",
          color: "#121615",
        }}
      >
        <main style={{ maxWidth: "32rem", textAlign: "center" }}>
          <h1 style={{ fontSize: 18, fontWeight: 600, margin: "0 0 0.5rem" }}>
            CatchX hit an unexpected error
          </h1>
          <p style={{ fontSize: 13, color: "#5c6663", lineHeight: 1.6, margin: "0 0 1.25rem" }}>
            {error.message || "Something went wrong while rendering this page."}
            {error.digest && (
              <>
                <br />
                <span style={{ fontFamily: "ui-monospace, monospace", fontSize: 11 }}>
                  digest {error.digest}
                </span>
              </>
            )}
          </p>
          <button
            onClick={reset}
            style={{
              height: 36,
              padding: "0 1rem",
              borderRadius: 8,
              border: "none",
              background: "#01a982",
              color: "#fff",
              fontSize: 13,
              fontWeight: 500,
              cursor: "pointer",
            }}
          >
            Try again
          </button>
        </main>
      </body>
    </html>
  );
}
