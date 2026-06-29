import Link from "next/link";

export function Footer() {
  return (
    <footer className="relative z-10 border-t border-white/[0.06] mt-16 py-6">
      <div className="max-w-[1600px] mx-auto px-6 flex flex-col sm:flex-row items-center justify-between gap-3">
        <span className="text-[11px] text-zinc-600 tracking-wide">
          © {new Date().getFullYear()} Aviation Hub · For flight simulation use only
        </span>
        <nav className="flex items-center gap-4 text-[11px] tracking-wide">
          <Link href="/terms" className="text-zinc-600 hover:text-zinc-400 transition-colors uppercase">
            Terms of Service
          </Link>
          <Link href="/privacy" className="text-zinc-600 hover:text-zinc-400 transition-colors uppercase">
            Privacy Policy
          </Link>
        </nav>
      </div>
    </footer>
  );
}
