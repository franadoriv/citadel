using System;
using System.IO;
using NUnit.Framework;
using UnityEngine;

namespace Citadel.Editor.Tests
{
    public sealed class CitadelProtocolAuthoritativeInputTests
    {
        [Serializable] private sealed class AuthoritativeInputFixture
        {
            public SequencedInputFixture sequenced_input;
            public InputReceiptFixture input_receipt;
            public InvalidFixture invalid;
        }

        [Serializable] private sealed class SequencedInputFixture
        {
            public string hex;
            public string token_hex;
            public string sequence_hex;
            public ushort original_custom_kind;
            public string opaque_body_hex;
        }

        [Serializable] private sealed class InputReceiptFixture
        {
            public string hex;
            public string match_id_hex;
            public string stream_id_hex;
            public string token_hex;
            public string acknowledged_sequence_hex;
            public string decided_sequence_hex;
            public byte disposition;
            public string authoritative_tick_hex;
            public bool correction_present;
            public string opaque_correction_hex;
        }

        [Serializable] private sealed class InvalidFixture
        {
            public string trailing_byte_hex;
            public int invalid_disposition_offset;
            public int invalid_correction_present_offset;
        }

        private static string FixturePath
        {
            get
            {
                for (var directory = new DirectoryInfo(UnityEngine.Application.dataPath); directory != null; directory = directory.Parent)
                {
                    string fromRepository = Path.Combine(directory.FullName, "clients", "authoritative-input-fixtures.json");
                    if (File.Exists(fromRepository)) return fromRepository;
                    string fromClientDirectory = Path.Combine(directory.FullName, "authoritative-input-fixtures.json");
                    if (File.Exists(fromClientDirectory)) return fromClientDirectory;
                }
                throw new FileNotFoundException("shared authoritative-input fixture was not found above the Unity project", "clients/authoritative-input-fixtures.json");
            }
        }

        [Test]
        public void InputReceipt_RoundTripsFullUnsignedCorrelationAndOpaqueCorrection()
        {
            AuthoritativeInputFixture fixture = JsonUtility.FromJson<AuthoritativeInputFixture>(File.ReadAllText(FixturePath));
            var receiptFixture = fixture.input_receipt;
            var token = Hex(receiptFixture.token_hex);
            var correction = Hex(receiptFixture.opaque_correction_hex);
            var receipt = new CitadelProtocol.InputReceipt(
                ReadUInt64(Hex(receiptFixture.match_id_hex)),
                ReadUInt64(Hex(receiptFixture.stream_id_hex)),
                token,
                ReadUInt64(Hex(receiptFixture.acknowledged_sequence_hex)),
                ReadUInt64(Hex(receiptFixture.decided_sequence_hex)),
                accepted: receiptFixture.disposition == 0,
                ReadUInt64(Hex(receiptFixture.authoritative_tick_hex)),
                correctionPresent: receiptFixture.correction_present,
                correction);

            Assert.That(CitadelProtocol.TryEncodeInputReceipt(receipt, out var encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(Hex(receiptFixture.hex)));
            Assert.That(CitadelProtocol.TryDecodeInputReceipt(encoded, encoded.Length, out var decoded), Is.True);
            Assert.That(decoded.MatchId, Is.EqualTo(ReadUInt64(Hex(receiptFixture.match_id_hex))));
            Assert.That(decoded.StreamId, Is.EqualTo(ReadUInt64(Hex(receiptFixture.stream_id_hex))));
            Assert.That(decoded.StreamToken, Is.EqualTo(token));
            Assert.That(decoded.AcknowledgedSequence, Is.EqualTo(ReadUInt64(Hex(receiptFixture.acknowledged_sequence_hex))));
            Assert.That(decoded.DecidedSequence, Is.EqualTo(ReadUInt64(Hex(receiptFixture.decided_sequence_hex))));
            Assert.That(decoded.Accepted, Is.EqualTo(receiptFixture.disposition == 0));
            Assert.That(decoded.AuthoritativeTick, Is.EqualTo(ReadUInt64(Hex(receiptFixture.authoritative_tick_hex))));
            Assert.That(decoded.CorrectionPresent, Is.EqualTo(receiptFixture.correction_present));
            Assert.That(decoded.Correction, Is.EqualTo(correction));

            Assert.That(CitadelProtocol.TryDecodeInputReceipt(encoded, encoded.Length - 1, out _), Is.False);
            var trailing = new byte[encoded.Length + 1]; encoded.CopyTo(trailing, 0);
            Assert.That(CitadelProtocol.TryDecodeInputReceipt(trailing, trailing.Length, out _), Is.False);
            encoded[fixture.invalid.invalid_disposition_offset] = 2;
            Assert.That(CitadelProtocol.TryDecodeInputReceipt(encoded, encoded.Length, out _), Is.False);
        }

        [Test]
        public void SequencedInput_RoundTripsUnsignedU64MaximumAndRejectsTrailingBytes()
        {
            AuthoritativeInputFixture fixture = JsonUtility.FromJson<AuthoritativeInputFixture>(File.ReadAllText(FixturePath));
            var inputFixture = fixture.sequenced_input;
            var token = Hex(inputFixture.token_hex);
            Assert.That(CitadelProtocol.TryEncodeSequencedInput(token, ReadUInt64(Hex(inputFixture.sequence_hex)), inputFixture.original_custom_kind, Hex(inputFixture.opaque_body_hex), out var encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(Hex(inputFixture.hex)));
            Assert.That(CitadelProtocol.TryDecodeSequencedInput(encoded, encoded.Length, out var sequence, out var kind, out var decodedToken, out var payload), Is.True);
            Assert.That(sequence, Is.EqualTo(ReadUInt64(Hex(inputFixture.sequence_hex))));
            Assert.That(kind, Is.EqualTo(inputFixture.original_custom_kind));
            Assert.That(decodedToken, Is.EqualTo(token));
            Assert.That(payload, Is.EqualTo(Hex(inputFixture.opaque_body_hex)));
            Assert.That(CitadelProtocol.TryDecodeSequencedInput(encoded, encoded.Length - 1, out _, out _, out _, out _), Is.False);
        }

        private static byte[] Hex(string value)
        {
            Assert.That(value, Is.Not.Null.And.Not.Empty);
            Assert.That(value.Length % 2, Is.EqualTo(0));
            var bytes = new byte[value.Length / 2];
            for (int index = 0; index < bytes.Length; index++) bytes[index] = Convert.ToByte(value.Substring(index * 2, 2), 16);
            return bytes;
        }

        private static ulong ReadUInt64(byte[] value)
        {
            Assert.That(value, Has.Length.EqualTo(8));
            ulong result = 0;
            foreach (byte part in value) result = (result << 8) | part;
            return result;
        }
    }
}
