import { cn } from "@/lib/utils";

interface Column<T> {
  key: keyof T | string;
  label: string;
  className?: string;
  render?: (row: T) => React.ReactNode;
}

interface TableListProps<T> {
  columns: Column<T>[];
  rows: T[];
  keyField: keyof T;
  className?: string;
  compact?: boolean;
}

export function TableList<T extends Record<string, unknown>>({
  columns,
  rows,
  keyField,
  className,
  compact = false,
}: TableListProps<T>) {
  return (
    <div className={cn("w-full overflow-x-auto", className)}>
      <table className="w-full text-left border-collapse">
        <thead>
          <tr className="border-b border-white/[0.06]">
            {columns.map((col) => (
              <th
                key={String(col.key)}
                className={cn(
                  "text-[9px] tracking-[0.2em] uppercase text-zinc-600 font-medium",
                  compact ? "pb-2 pr-4" : "pb-3 pr-6",
                  col.className
                )}
              >
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={String(row[keyField])}
              className="hub-row"
            >
              {columns.map((col) => (
                <td
                  key={String(col.key)}
                  className={cn(
                    "text-xs text-zinc-300",
                    compact ? "py-1.5 pr-4" : "py-2.5 pr-6",
                    col.className
                  )}
                >
                  {col.render
                    ? col.render(row)
                    : String(row[col.key as keyof T] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && (
        <p className="text-center text-[11px] text-zinc-700 py-6 tracking-widest uppercase">
          No data
        </p>
      )}
    </div>
  );
}
