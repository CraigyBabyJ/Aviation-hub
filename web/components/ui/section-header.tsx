import { cn } from "@/lib/utils";

interface SectionHeaderProps {
  title: string;
  subtitle?: string;
  live?: boolean;
  count?: number | string;
  className?: string;
  size?: "sm" | "md" | "lg";
}

export function SectionHeader({
  title,
  subtitle,
  live = false,
  count,
  className,
  size = "md",
}: SectionHeaderProps) {
  return (
    <div className={cn("flex items-start justify-between gap-3 mb-4", className)}>
      <div className="flex items-center gap-3">
        {/* Live dot */}
        {live && (
          <span className="live-dot mt-0.5 flex-shrink-0 w-1.5 h-1.5 rounded-full bg-red-500 block" />
        )}

        <div>
          <h2
            className={cn(
              "font-bold tracking-[0.2em] uppercase text-white leading-none",
              size === "lg" && "text-xl",
              size === "md" && "text-sm",
              size === "sm" && "text-xs"
            )}
          >
            {title}
          </h2>
          {subtitle && (
            <p className="text-[10px] text-zinc-500 tracking-wider mt-1 uppercase">
              {subtitle}
            </p>
          )}
        </div>
      </div>

      {/* Count badge */}
      {count !== undefined && (
        <span className="flex-shrink-0 px-2 py-0.5 text-[10px] tracking-widest uppercase border border-white/[0.08] text-zinc-400 rounded">
          {count}
        </span>
      )}
    </div>
  );
}
