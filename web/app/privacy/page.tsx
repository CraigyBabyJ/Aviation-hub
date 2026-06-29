import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Privacy Policy – Aviation Hub",
  description: "Privacy Policy for Aviation Hub and the Aviation Hub Discord Bot",
};

export default function PrivacyPage() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12 text-gray-200">
      <h1 className="text-3xl font-bold text-white mb-2">Privacy Policy</h1>
      <p className="text-sm text-gray-400 mb-10">Last updated: 29 June 2026</p>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">1. Who We Are</h2>
        <p>
          Aviation Hub ("we", "us", "our") is a hobby project operated by Craig. This policy explains
          what data we collect when you use the Aviation Hub website and/or the Aviation Hub Discord Bot,
          how we use it, and your rights regarding that data.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">2. Data We Collect</h2>

        <h3 className="text-lg font-medium text-white mt-4 mb-2">2a. Website</h3>
        <p className="mb-2">
          The Aviation Hub website does not require you to create an account or log in. We do not
          intentionally collect personally identifiable information from website visitors.
        </p>
        <p>
          Standard web server logs (IP address, browser user-agent, requested URL, timestamp) may be
          retained for up to 30 days for security and debugging purposes and are not shared with third
          parties.
        </p>

        <h3 className="text-lg font-medium text-white mt-4 mb-2">2b. Discord Bot</h3>
        <p className="mb-2">
          When the Bot is added to a Discord server or a slash command is used, we may process and
          store the following:
        </p>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>
            <strong className="text-white">Discord Guild (server) ID</strong> – stored when a server
            configures airport alert notifications, so we know where to send them.
          </li>
          <li>
            <strong className="text-white">Discord Channel ID</strong> – stored alongside the Guild ID
            for the same alert-delivery purpose.
          </li>
          <li>
            <strong className="text-white">Discord User ID</strong> – logged transiently alongside
            command interactions for debugging; not stored persistently.
          </li>
          <li>
            <strong className="text-white">Command inputs (e.g. ICAO codes)</strong> – used only to
            fulfil the request and not stored beyond the lifetime of the request.
          </li>
        </ul>
        <p className="mt-3">
          We do not collect your Discord username, avatar, email address, or any other personal
          profile information.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">3. How We Use Your Data</h2>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>To deliver the Bot's slash command responses.</li>
          <li>To send configured airport ATC alert notifications to the correct Discord channel.</li>
          <li>To diagnose errors and maintain service stability.</li>
        </ul>
        <p className="mt-3">We do not use your data for advertising, profiling, or marketing.</p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">4. Third-Party Services</h2>
        <p className="mb-2">
          Aviation Hub retrieves data from the following third-party services. Using Aviation Hub means
          data requests are made to these services on your behalf:
        </p>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>VATSIM (vatsim.net) – live network data</li>
          <li>IVAO (ivao.aero) – live network data</li>
          <li>OurAirports (ourairports.com) – airport/runway reference data</li>
          <li>Aviation weather providers – METAR, TAF, ATIS, SIGMET feeds</li>
          <li>ESRI World Imagery – satellite tile imagery for the /sat command</li>
        </ul>
        <p className="mt-3">
          Each service has its own privacy policy. We are not responsible for how these third parties
          handle data. Satellite images fetched via /sat are cached locally on our server to reduce
          repeat requests to ESRI.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">5. Data Retention</h2>
        <ul className="list-disc list-inside space-y-1 text-gray-300">
          <li>Alert configuration (Guild ID + Channel ID): retained until the Bot is removed from the server or the configuration is cleared.</li>
          <li>Web server logs: up to 30 days.</li>
          <li>All other interaction data: not stored persistently.</li>
        </ul>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">6. Data Sharing</h2>
        <p>
          We do not sell, rent, or share your data with third parties, except where required by law or
          to protect the rights and safety of the service or others.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">7. Your Rights</h2>
        <p className="mb-3">
          If you are located in the United Kingdom or European Economic Area, you have rights under UK
          GDPR / GDPR including the right to access, correct, or request deletion of personal data we
          hold about you. To exercise these rights, or to request removal of a Discord server's alert
          configuration, contact us at the address below.
        </p>
        <p>
          Because we do not link Guild/Channel IDs to individual identities, the most effective way to
          remove Bot data is to remove the Bot from your Discord server.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">8. Children</h2>
        <p>
          Aviation Hub is not directed at children under the age of 13 (or the applicable minimum age
          in your country). We do not knowingly collect data from children.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">9. Changes to This Policy</h2>
        <p>
          We may update this Privacy Policy from time to time. The "Last updated" date at the top of
          this page will reflect any changes. Continued use of the service after changes are posted
          constitutes acceptance of the updated policy.
        </p>
      </section>

      <section className="mb-8">
        <h2 className="text-xl font-semibold text-white mb-3">10. Contact</h2>
        <p>
          For any privacy-related questions or requests, please contact us at{" "}
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
        <Link href="/terms" className="text-blue-400 hover:underline">
          Terms of Service
        </Link>
        .
      </p>
    </div>
  );
}
