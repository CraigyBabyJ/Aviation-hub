import type { Metadata } from "next";
import "./globals.css";
import { Navbar } from "@/components/layout/navbar";
import { Background } from "@/components/layout/background";

export const metadata: Metadata = {
  title: "Aviation Hub",
  description: "Premium VATSIM aviation intelligence dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body>
        <Background />
        <Navbar />
        <main className="relative z-10 pt-14 min-h-screen">
          {children}
        </main>
      </body>
    </html>
  );
}
