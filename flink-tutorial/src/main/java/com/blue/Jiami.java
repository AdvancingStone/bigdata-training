package com.blue;

import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Security;

public class Jiami {

    public static void main(String[] args) {
        String base64Aes256Key = "4iJP2D4RWxtt+ynUsv2CV5l7FSb1Q5l0alQDocdZxQk=";
//        String key = "{\"1201\":\"unionId\",\"1202\":\"opneId Wx\",\"1203\":\"openId MINI-WECHAT\"}";
        String key = "abcdef";
        String result = encrypt(key, base64Aes256Key);
        System.out.println(result);

        String old = decrypt(result, base64Aes256Key);
        System.out.println(old);
    }
    /**
     * 加密
     * @param content
     * @param base64Aes256Key
     * @return
     */
    public static String encrypt(String content, String base64Aes256Key) {
        try {
            SecretKeySpec key = new SecretKeySpec(
                    Base64.decodeBase64(base64Aes256Key.getBytes("UTF-8")), "AES");
            Security.addProvider(new BouncyCastleProvider());
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS7Padding",
                    "BC");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            byte[] byteContent = content.getBytes("utf-8");
            byte[] cryptograph = cipher.doFinal(byteContent);
            return new String(Base64.encodeBase64(cryptograph,
                    false),"UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 解密
     * @param cipherText
     * @param base64Aes256Key
     * @return
     */

    public static String decrypt(String cipherText, String
            base64Aes256Key) {
        try {
            SecretKeySpec key = new SecretKeySpec(
                    Base64.decodeBase64(base64Aes256Key.getBytes("UTF-8")), "AES");
            Security.addProvider(new BouncyCastleProvider());
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS7Padding",
                    "BC");
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] content =
                    cipher.doFinal(Base64.decodeBase64(cipherText.getBytes("utf-8")));
            return new String(content, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
