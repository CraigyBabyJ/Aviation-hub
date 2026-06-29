import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Terms of Service – Aviation Hub",
  description: "Terms of Service for Aviation Hub and the Aviation Hub Discord Bot",
};

export default function TermsPage() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12 text-gray-200">
      <h1 className="text-3xl font-bold text-white mb-2">Terms of Service</h1>
      <p className="text-sm text-gray-400 mb-10">Last updated: 29 June 2026</p>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">1. Acceptance</h2>
        <p>
          By accessing Aviation Hub (the "Website") or adding the Aviation Hub Discord Bot (the "Bot") to
          a Discord server, you agree to these Terms of Service ("Terms"). If you do not agree, do not use
          the service.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">2. Description of Service</h2>
        <p className="mb-3">
          Aviation Hub is a hobby project that aggregates and displays real-time data from the VATSIM and
          IVAO online flight simulation networks, together with publicly available aviation weather data
          (METAR, TAF, ATIS, SIGMET) and airport/runway information from OurAirports and similar open
          datasets. The Bot exposes this data via Discord slash commands.
        </p>
        <p>
          Aviation Hub is provided free of charge on a best-effort basis.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">3. Simulation Use Only</h2>
        <p className="mb-3 font-semibold text-yellow-400">
          Aviation Hub is intended solely for use with online flight simulation networks (VATSIM, IVAO).
          It must never be used for real-world aviation navigation, flight planning, weather briefings,
          or any other safety-of-flight purpose.
        </p>
        <p>
          All data is provided for entertainment and simulation reference only. Weather data, NOTAM
          information, and ATC status displayed may be delayed, incomplete, or inaccurate. Always use
          official, certified sources for any real-world aviation activity.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">4. Third-Party Data</h2>
        <p className="mb-2">Aviation Hub relies on data provided by third parties, including:</p>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>VATSIM (vatsim.net) – live ATC, pilot, event, and booking data</li>
          <li>IVAO (ivao.aero) – live ATC data</li>
          <li>OurAirports (ourairports.com) – airport and runway data</li>
          <li>Aviation weather services – METAR, TAF, ATIS, SIGMET</li>
          <li>ESRI World Imagery – satellite airport imagery</li>
        </ul>
        <p className="mt-3">
          Aviation Hub is not affiliated with, endorsed by, or officially connected to any of these
          organisations. Their respective terms of service apply to their data.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">5. Acceptable Use</h2>
        <p className="mb-2">You agree not to:</p>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>Attempt to reverse-engineer, scrape, or overload the service.</li>
          <li>Use the service in violation of Discord's Terms of Service or Community Guidelines.</li>
          <li>Resell or redistribute Aviation Hub data without permission.</li>
          <li>Use the service for any unlawful purpose.</li>
        </ul>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">6. No Warranty</h2>
        <p>
          The service is provided "as is" and "as available" without any warranty of any kind, express or
          implied. We do not guarantee uptime, data accuracy, or fitness for any particular purpose.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">7. Limitation of Liability</h2>
        <p>
          To the maximum extent permitted by applicable law, Aviation Hub and its operator shall not be
          liable for any direct, indirect, incidental, special, or consequential damages arising from your
          use of, or inability to use, the service or reliance on any data provided.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">8. Changes to the Service</h2>
        <p>
          We may modify or discontinue the service at any time without notice. We may also update these
          Terms at any time; continued use of the service after changes are posted constitutes acceptance
          of the new Terms.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">9. Governing Law</h2>
        <p>
          These Terms are governed by the laws of England and Wales. Any disputes shall be subject to the
          exclusive jurisdiction of the courts of England and Wales.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">10. Contact</h2>
        <p>
          Questions about these Terms can be sent to{" "}
          <a href="mailto:support@craigybabyj.com" className="text-blue-400 hover:underline">
            support@craigybabyj.com
          </a>{" "}or join our{" "}
          <a href="https://discord.craigybabyj.com" className="text-blue-400 hover:underline" target="_blank" rel="noreferrer">
            Discord server
          </a>.
        </p>
      </section>

      <p className="text-sm text-gray-500 mt-10">
        See also our{" "}
        <Link href="/privacy" className="text-blue-400 hover:underline">
          Privacy Policy
        </Link>
        .
      </p>
    </div>
  );
}
