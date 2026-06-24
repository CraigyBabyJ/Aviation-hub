import Link from "next/link";

export default function NotFound() {
  return (
    <div className="min-h-screen flex items-center justify-center px-6">
      <div className="text-center space-y-4">
        <p className="text-[9px] tracking-[0.3em] uppercase text-red-500/70">404</p>
        <h1 className="text-4xl font-bold tracking-[0.15em] uppercase text-white">
          Not Found
        </h1>
        <p className="text-[11px] text-zinc-600 tracking-wide">
          This route doesn&apos;t exist on the Aviation Hub.
        </p>
        <Link
          href="/dashboard"
          className="inline-flex items-center gap-2 mt-4 px-4 py-2 rounded-lg border border-white/[0.08] bg-white/[0.02] hover:bg-white/[0.05] text-[11px] tracking-widest uppercase text-zinc-400 hover:text-white transition-colors"
        >
          Return to Dashboard
        </Link>
      </div>
    </div>
  );
}
