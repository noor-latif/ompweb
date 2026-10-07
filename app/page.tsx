import { Suspense } from "react";
import { headers } from "next/headers";
import { AppShell } from "@/components/AppShell";
import { getInstallName } from "@/lib/install-name";
// Locale dictionaries are plain JSON: the server component can read the real
// empty-session heading directly, painted pre-hydration.
import en from "@/lib/i18n/locales/en.json";
import ja from "@/lib/i18n/locales/ja.json";
import zhCN from "@/lib/i18n/locales/zh-CN.json";

export default async function Home() {
  const appName = getInstallName(await headers());
  return (
    <Suspense fallback={<RouteLoadingFallback />}>
      <AppShell appName={appName} />
    </Suspense>
  );
}

function RouteLoadingFallback() {
  return (
    <div className="route-fallback" role="status" aria-busy="true" aria-label="Loading omp web">
      <aside className="route-fallback-sidebar" aria-hidden="true">
        <div className="skeleton route-fallback-sidebar-title" />
        <div className="skeleton route-fallback-sidebar-button" />
        <div className="route-fallback-sidebar-list">
          <div className="skeleton route-fallback-sidebar-row" />
          <div className="skeleton route-fallback-sidebar-row" />
          <div className="skeleton route-fallback-sidebar-row" />
        </div>
      </aside>
      <main className="route-fallback-main" aria-hidden="true">
        <div className="route-fallback-topbar">
          <div className="skeleton route-fallback-topbar-control" />
          <div className="skeleton route-fallback-topbar-title" />
          <div className="skeleton route-fallback-topbar-control" />
        </div>
        <div className="route-fallback-content">
          {/* The real empty-session heading painted pre-hydration: this is
              the LCP element (FCP too — plain skeleton divs are never
              contentful). Slightly larger than ChatWindow's h1 so it stays
              the LCP after hydration. html[lang] is set by the head
              bootstrap script before first paint. */}
          <p className="route-fallback-heading display-serif" aria-hidden="true">
            <span data-loc="en">{en["appShell.newSessionTitle"]}</span>
            <span data-loc="zh-CN">{zhCN["appShell.newSessionTitle"]}</span>
            <span data-loc="ja">{ja["appShell.newSessionTitle"]}</span>
          </p>
          <div className="skeleton route-fallback-line route-fallback-line-short" />
          <div className="skeleton route-fallback-line" />
          <div className="skeleton route-fallback-line" />
          <div className="skeleton route-fallback-composer" />
        </div>
      </main>
    </div>
  );
}
