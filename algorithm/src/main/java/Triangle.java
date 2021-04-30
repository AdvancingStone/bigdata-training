import java.util.Scanner;

/**
 * 打印如下三角形
 * n=5
 * 1
 * 2 12
 * 3 13 11
 * 4 14 15 10
 * 5 6  7  8  9
 */

public class Triangle {
    private static int value = 1;
    public static void main(String[] args) {
        System.out.println("请输入整数 N：");
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[][] arr = new int[n][n];
        int max_value = n*(n+1)/2;
        System.out.println(max_value);
        int x=0;

        while(value <= max_value){
            int down = down(arr, x, n);
            int[] right = right(arr, down, ++x, n);
            int upleft = upleft(arr, right, x, n);
            System.out.println("========================================="+x);
        }

        printTriangle(arr, n);
    }

    private static int upleft(int[][] arr, int a[], int k, int n) {
        System.out.println("upleft-> x:"+a[0]+ " y:"+a[1]);
        int x;
        int y;
        for(x=a[0], y=a[1]; x>=0&&y>=0; x--,y--){
            if(arr[x][y] == 0){
                arr[x][y] = value++;
            }else {
                break;
            }
        }
        printTriangle(arr, n);
        return x+1;
    }

    private static int[] right(int[][] arr, int x, int y, int n) {
        System.out.println("right->  x:" + x + " y:"+y);
        for(; y<=x && x<n; y++){
            if(arr[x][y] == 0){
                arr[x][y] = value++;
            }else{
                break;
            }
        }
        printTriangle(arr, n);
        int[] res = {x-1, y-2};
        return res;
    }

    private static int down(int[][] arr, int x, int n){
        System.out.println("down-> x:"+x);
        int y=x;
        for(; x<n-y; x++){
            if(arr[x][y] == 0){
                arr[x][y] = value++;
            }
        }
        printTriangle(arr, n);
        return x-1;

    }

    private static void printTriangle(int[][] arr, int n){
        for(int x=0; x<n; x++){
            for(int y=0; y<n; y++){
                if(arr[x][y]!=0){
                    System.out.print(arr[x][y] + "\t");
                }else{
                    System.out.print("\t");
                }
            }
            System.out.println();
        }
    }
}
