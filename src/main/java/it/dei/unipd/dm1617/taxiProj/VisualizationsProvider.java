package it.dei.unipd.dm1617.taxiProj;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public class VisualizationsProvider
{
	public static BufferedImage draw(JavaRDD<Position> pos, int imgWidth, int imgHeight,
						double topLeftLatitude, double topLeftLongitude,
						double bottomRightLatitude, double bottomRightLongitude)
	{
		BufferedImage img = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB_PRE);
		Graphics2D graphics = img.createGraphics();
		graphics.setPaint (Color.black );
		graphics.fillRect (0, 0, img.getWidth(), img.getHeight());		
		
		List<Position> pa = pos.collect();//brutto come poche cose
		for (Position p : pa)
		{
			int x = (int)Math.round((p.getPickupLongitude() - topLeftLongitude)/(bottomRightLongitude - topLeftLongitude)*imgWidth); 
			int y = (int)Math.round((topLeftLatitude - p.getPickupLatitude())/(topLeftLatitude - bottomRightLatitude)*imgHeight);
			//bianco semitrasparente
			double a = 0.5;
			int r = 0xff;
			int g = 0xff;
			int b = 0xff;
			
			int c0=0;
			try
			{
				c0 = img.getRGB(x, y);
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				/*
				System.out.println("p.getPickupLongitude()="+p.getPickupLongitude()+"  p.getPickupLatitude()="+p.getPickupLatitude()+"  x="+x+" y="+y);
				System.out.println("p.getPickupLatitude()="+p.getPickupLatitude()+"  x="+x+" y="+y);
				System.out.println("topLeftLatitude="+topLeftLatitude+"  topLeftLongitude="+topLeftLongitude+"  bottomRightLatitude="+bottomRightLatitude+" bottomRightLongitude="+bottomRightLongitude);
				throw new RuntimeException("Coordinate out of bounds!");*/
				System.out.println("position ("+p.getPickupLongitude()+"; "+p.getPickupLatitude()+") out of image area");
				continue;
			}
			
			int c = 0xff<<24 |
					( (int)( ((c0 & 0xff0000)>>16) * a + r*(1-a) ) )<<16 |
					( (int)( ((c0 & 0xff00)  >>8 ) * a + g*(1-a) ) )<<8  |
					( (int)( ((c0 & 0xff)        ) * a + b*(1-a) ) ) ;						
			img.setRGB(x, y, c);
		}
		
		return img;
	}
	
}
