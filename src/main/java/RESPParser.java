import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RESPParser {
    private final InputStream inputStream;
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RESPParser(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public Object parse() throws IOException {
        int firstByte = inputStream.read();

        if (firstByte == -1 ){
            return null;
        }

        switch (firstByte) {
            case '+' -> {
                return parseSimpleString();
            }
            case '-' -> {
                return parseError();
            }
            case ':' -> {
                return parseInteger();
            }
            case '$' -> {
                return parseBulkString();
            }
            case '*' -> {
                return parseArray();

            }
            default -> {
                throw new IOException("Unknown RESP type prefix: " + (char) firstByte);
            }
        }
    }


    private byte[] readLineBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = inputStream.read()) != -1) {
            if (b == CR) {
                int nextByte = inputStream.read();
                if (nextByte == -1) {
                    throw new IOException("Malformed RESP: Unexpected EOF after CR");
                }
                if (nextByte == LF) {
                    return baos.toByteArray();
                } else {
                    baos.write(b);
                    baos.write(nextByte);
                }
            } else {
                baos.write(b);
            }
        }
        throw new IOException("Malformed RESP: Unexpected EOF after CRLF");
    }

    private Object parseSimpleString() throws IOException {
        byte[] lineBytes = readLineBytes();
        return new String(lineBytes, StandardCharsets.UTF_8);
    }

    private Object parseError() throws IOException {
        byte[] lineBytes = readLineBytes();
        return new String(lineBytes, StandardCharsets.UTF_8);
    }

    private Long parseInteger() throws IOException {
        byte[] lineBytes = readLineBytes();
        String intStr = new String(lineBytes, StandardCharsets.UTF_8);
        try {
            return Long.parseLong(intStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP: Invalid integer: " + intStr, e);
        }
    }

    private String parseBulkString() throws IOException {
        byte[] lineBytes = readLineBytes();
        String lengthStr = new String(lineBytes, StandardCharsets.UTF_8);
        long length;

        try {
            length = Long.parseLong(lengthStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP: Invalid bulk string length: " + lengthStr);
        }

        if (length == -1){
            return null;
        }

        if (length < 0) {
            throw new IOException("Malformed RESP: Negative bulk string length: " + length);
        }

        byte[] data = new byte[(int) length];
        int bytesRead = inputStream.read(data);

        if (bytesRead != length) {
            throw new IOException("Malformed RESP: Malformed RESP length: " + bytesRead);
        }

        int cr = inputStream.read();
        int lf = inputStream.read();

        if (cr != CR || lf != LF) {
            throw new IOException("Malformed RESP: Malformed RESP CR/LF length: " + cr);
        }

        return new String(data, StandardCharsets.UTF_8);
    }

    private List<Object> parseArray() throws IOException {
        byte[] lineBytes = readLineBytes();
        String lengthStr = new String(lineBytes, StandardCharsets.UTF_8);
        long numElements;
        try {
            numElements = Long.parseLong(lengthStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP: Invalid length: " + lengthStr);
        }

        if (numElements == -1) {
            return null;
        }

        if (numElements < 0) {
            throw new IOException("Malformed RESP: Invalid length: " + numElements);
        }

        List<Object> elements = new ArrayList<>((int) numElements);
        for (int i = 0; i < numElements; i++) {
            Object element = parse();
            if (element == null && numElements > 0){
                throw new IOException("Malformed RESP: Unexpected EOF while parsing array element " + i);
            }

            elements.add(element);
        }
        return elements;
    }
}
