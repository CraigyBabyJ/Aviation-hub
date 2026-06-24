import { cn } from "@/lib/utils";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface StatCardProps {
  label: string;
  value: string | number;
  subtext?: string;
  trend?: "up" | "down" | "neutral";
  accent?: boolean;
  className?: string;
}

export function StatCard({
  label,
  value,
  subtext,
  trend,
  accent = false,
  className,
}: StatCardProps) {
  const TrendIcon =
    trend === "up" ? TrendingUp : trend === "down" ? TrendingDown : Minus;
  const trendColor =
    trend === "up" ? "text-white" : trend === "down" ? "text-red-500" : "text-zinc-600";

  return (
    <div
      className={cn(
        "flex flex-col gap-1 px-4 py-3 rounded-lg border border-white/[0.06] bg-white/[0.02]",
        accent && "border-red-600/20 bg-red-950/10",
        className
      )}
    >
      <span className="text-[9px] tracking-[0.22em] uppercase text-zinc-500 font-medium">
        {label}
      </span>
      <div className="flex items-end justify-between gap-2">
        <span
          className={cn(
            "text-2xl font-bold leading-none tabular-nums",
            accent ? "text-red-400" : "text-white"
          )}
        >
          {value}
        </span>
        {trend && (
          <TrendIcon size={14} className={cn(trendColor, "mb-0.5")} strokeWidth={1.5} />
        )}
      </div>
      {subtext && (
        <span className="text-[10px] text-zinc-600 tracking-wide">{subtext}</span>
      )}
    </div>
  );
}
